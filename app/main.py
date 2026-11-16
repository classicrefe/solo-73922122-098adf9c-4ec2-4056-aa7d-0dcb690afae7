"""FastAPI 网页与 API。

- 网页：项目列表/创建、按项目上传 ZIP、显示包状态与拒收原因、下载处理包、
  撤回已发布包（含受影响包与关联依据预览、撤回结果展示）。
- JSON API：供脚本（跨批引用/撤回演示）与双实例并发演示使用。
- 处理在后台线程池执行，转换期间其他上传与下载不受影响。
- 存量包成员/引用关系补建在后台线程执行，完成前撤回入口只读。
"""
from __future__ import annotations

import html
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from . import pipeline
from .store import STATUS_LABELS, Store

log = logging.getLogger("deid.web")

PAGE_CSS = """
body{font-family:-apple-system,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
margin:2rem auto;max-width:980px;color:#222}
h1{font-size:1.4rem} h2{font-size:1.1rem;margin-top:2rem}
table{border-collapse:collapse;width:100%;font-size:.9rem}
th,td{border:1px solid #ddd;padding:.4rem .6rem;text-align:left;vertical-align:top}
th{background:#f5f5f5}
.status-published{color:#137333;font-weight:600}
.status-rejected,.status-failed{color:#b3261e;font-weight:600}
.status-withdrawn{color:#7a1f1f;font-weight:700}
.status-processing,.status-received{color:#b06000;font-weight:600}
form.inline{display:inline} input[type=text]{padding:.35rem;width:20rem}
textarea{padding:.35rem;width:32rem;height:5rem}
button{padding:.35rem .9rem} .muted{color:#777;font-size:.85rem}
.reason{white-space:pre-wrap;word-break:break-all}
.banner{background:#fff4e5;border:1px solid #f0c36d;padding:.7rem 1rem;border-radius:6px}
.danger{background:#fdecea;border:1px solid #f5b7b1;padding:.7rem 1rem;border-radius:6px}
.okbox{background:#e6f4ea;border:1px solid #a8dab5;padding:.7rem 1rem;border-radius:6px}
.basis{font-size:.85rem;color:#444}
"""

# 进程内补建进度（权威状态在 SQLite app_meta，这里只用于页面展示进度）
_backfill_state = {"running": False, "done": False, "processed": 0,
                   "pending": 0, "failed": [], "started": False}
_backfill_lock = threading.Lock()


def _reset_backfill_state() -> None:
    """重置进程内补建状态（主要供测试中为新 Store 重新建 app 时使用）。"""
    with _backfill_lock:
        _backfill_state.update(running=False, done=False, processed=0,
                               pending=0, failed=[], started=False)


def _esc(s) -> str:
    return html.escape(str(s if s is not None else ""))


def _page(title: str, body: str, refresh: bool = False) -> str:
    meta = '<meta http-equiv="refresh" content="3">' if refresh else ""
    return (
        f"<!doctype html><html lang=zh><head><meta charset=utf-8>{meta}"
        f"<title>{_esc(title)}</title><style>{PAGE_CSS}</style></head>"
        f"<body><h1>{_esc(title)}</h1>{body}</body></html>"
    )


def _status_html(status: str) -> str:
    label = STATUS_LABELS.get(status, status)
    return f'<span class="status-{_esc(status)}">{_esc(label)}</span>'


def _start_backfill(store: Store) -> None:
    """后台补建存量包成员/引用关系；重复启动安全（任务幂等）。"""
    with _backfill_lock:
        if _backfill_state["started"]:
            return
        _backfill_state["started"] = True
        pending = len(store.pending_backfill_package_ids())
        _backfill_state["pending"] = pending
        # 无待补建包（全新库或已补建完成）：直接标记完成，不启动线程
        if pending == 0 and store.withdrawal_ready():
            _backfill_state["running"] = False
            _backfill_state["done"] = True
            return
        _backfill_state["running"] = True

    def _run() -> None:
        try:
            summary = store.backfill_pending_packages()
            with _backfill_lock:
                _backfill_state["processed"] = summary["processed"]
                _backfill_state["failed"] = summary["failed"]
                _backfill_state["pending"] = len(store.pending_backfill_package_ids())
                _backfill_state["done"] = store.withdrawal_ready()
                _backfill_state["running"] = False
            log.info("backfill finished: %s", summary)
        except Exception:
            log.exception("backfill task crashed")
            with _backfill_lock:
                _backfill_state["running"] = False

    threading.Thread(target=_run, name="backfill", daemon=True).start()


def create_app(store: Store | None = None) -> FastAPI:
    store = store or Store(os.environ.get("DEID_DATA_DIR", "./data"))
    store.init()
    app = FastAPI(title="DICOM 去标识化与撤回平台")
    executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="deid-worker")
    _start_backfill(store)

    def _backfill_banner() -> str:
        with _backfill_lock:
            state = dict(_backfill_state)
        if state["done"] and store.withdrawal_ready():
            return ""
        if state["running"]:
            return (
                '<div class=banner>存量包成员与序列引用关系补建进行中'
                f'（待补建 {state["pending"]} 个包），期间上传下载正常，'
                "撤回功能将在补建完成后开放。</div>"
            )
        failed_n = len(state["failed"])
        tip = f"，{failed_n} 个包原件解析失败" if failed_n else ""
        return (
            f'<div class=banner>撤回功能暂未开放：存量包关系补建未完成{tip}，'
            "页面将自动重试刷新。上传与下载不受影响。</div>"
        )

    # ---------------- 网页 ----------------
    @app.get("/", response_class=HTMLResponse)
    def index():
        rows = "".join(
            "<tr>"
            f'<td><a href="/projects/{_esc(p["project_id"])}">{_esc(p["project_id"])}</a></td>'
            f"<td>{_esc(p['name'])}</td>"
            f"<td>{p['package_count']}</td><td>{p['instance_count']}</td>"
            f"<td class=muted>{_esc(p['created_at'])}</td></tr>"
            for p in store.list_projects()
        ) or '<tr><td colspan=5 class=muted>暂无项目</td></tr>'
        body = f"""
        <h2>新建研究项目</h2>
        <form method=post action="/projects">
          <input type=text name=name placeholder="项目名称（如：肺癌纵向队列）" required>
          <button type=submit>创建</button>
        </form>
        <h2>项目列表</h2>
        <table><tr><th>项目ID</th><th>名称</th><th>包数</th><th>已发布实例</th><th>创建时间</th></tr>
        {rows}</table>"""
        return _page("DICOM 去标识化与撤回平台", body)

    @app.post("/projects")
    async def create_project(request: Request):
        form = await request.form()
        name = str(form.get("name") or "").strip()
        if not name:
            raise HTTPException(400, "项目名称不能为空")
        project_id = store.create_project(name)
        return RedirectResponse(f"/projects/{project_id}", status_code=303)

    @app.get("/projects/{project_id}", response_class=HTMLResponse)
    def project_page(project_id: str):
        project = store.get_project(project_id)
        if not project:
            raise HTTPException(404, "项目不存在")
        packages = store.list_packages(project_id)
        rows = ""
        for p in packages:
            download = (
                f'<a href="/packages/{_esc(p["package_id"])}/download">下载处理包</a>'
                if p["status"] == "published" else ""
            )
            withdraw = (
                f'<a href="/packages/{_esc(p["package_id"])}/withdraw">整包撤回</a>'
                if p["status"] == "published" else ""
            )
            rows += (
                "<tr>"
                f"<td class=muted>{_esc(p['package_id'])}</td>"
                f"<td>{_esc(p['original_name'])}</td>"
                f"<td>{_status_html(p['status'])}</td>"
                f"<td>{p['instance_count']}</td>"
                f'<td class=reason>{_esc(p["reason"])}</td>'
                f"<td>{download} {withdraw}</td>"
                f"<td class=muted>{_esc(p['created_at'])}</td></tr>"
            )
        if not rows:
            rows = '<tr><td colspan=7 class=muted>暂无上传记录</td></tr>'
        busy = any(p["status"] in ("received", "processing") for p in packages)
        withdrawn_count = sum(1 for p in packages if p["status"] == "withdrawn")
        body = f"""
        <p class=muted><a href="/">&larr; 返回项目列表</a></p>
        {_backfill_banner()}
        <h2>上传检查 ZIP（1~20 份单帧未压缩 DICOM，不得含像素烧录文字）</h2>
        <form method=post action="/projects/{_esc(project_id)}/packages"
              enctype=multipart/form-data>
          <input type=file name=file accept=".zip" required>
          <button type=submit>上传</button>
        </form>
        <h2>上传记录（已发布实例共 {store.instance_count(project_id)} 个，
        已撤回包 {withdrawn_count} 个）</h2>
        <table><tr><th>包ID</th><th>原始文件</th><th>状态</th><th>实例数</th>
        <th>拒收/撤回原因</th><th>操作</th><th>上传时间</th></tr>{rows}</table>
        <h2>撤回结论</h2>
        {_withdrawals_table(project_id)}"""
        return _page(f"项目 {project['name']}（{project_id}）", body,
                     refresh=busy or _backfill_state["running"])

    def _withdrawals_table(project_id: str) -> str:
        records = store.list_withdrawals(project_id)
        if not records:
            return '<p class=muted>暂无撤回记录。</p>'
        body_rows = "".join(
            "<tr>"
            f"<td class=muted>{_esc(r['package_id'])}</td>"
            f"<td>{'人工撤回入口' if r['is_root'] else '连带撤回'}</td>"
            f"<td class=muted>{_esc(r['root_package_id'])}</td>"
            f"<td class=reason>{_esc(r['reason'])}</td>"
            f"<td class=basis>{_esc(r['basis'])}</td>"
            f"<td class=muted>{_esc(r['created_at'])}</td></tr>"
            for r in records
        )
        return (
            "<table><tr><th>包ID</th><th>类型</th><th>撤回源头</th>"
            "<th>原因</th><th>关联依据</th><th>时间</th></tr>"
            f"{body_rows}</table>"
        )

    @app.post("/projects/{project_id}/packages")
    def upload_package(project_id: str, file: UploadFile = File(...)):
        _save_and_enqueue(project_id, file)
        return RedirectResponse(f"/projects/{project_id}", status_code=303)

    @app.get("/packages/{package_id}/download")
    def download_package(package_id: str):
        pkg = store.get_package(package_id)
        if not pkg:
            raise HTTPException(404, "包不存在")
        if pkg["status"] == "withdrawn":
            raise HTTPException(
                409, f"包已永久停供（{STATUS_LABELS['withdrawn']}），"
                f"撤回原因：{pkg.get('reason') or '无'}。新下载请求一律拒绝")
        if pkg["status"] != "published":
            raise HTTPException(
                409, f"包未发布（当前状态：{STATUS_LABELS.get(pkg['status'])}），"
                f"原因：{pkg.get('reason') or '无'}")
        path = store.packages_dir / f"{package_id}.zip"
        if not path.exists():
            raise HTTPException(404, "处理包文件缺失")
        return FileResponse(
            path, media_type="application/zip",
            filename=f"deidentified_{pkg['project_id']}_{package_id}.zip")

    # ---------------- 撤回网页 ----------------
    @app.get("/packages/{package_id}/withdraw", response_class=HTMLResponse)
    def withdraw_form(package_id: str):
        pkg = store.get_package(package_id)
        if not pkg:
            raise HTTPException(404, "包不存在")
        if pkg["status"] != "published":
            raise HTTPException(
                409, f"仅已发布包可以撤回，当前状态："
                f"{STATUS_LABELS.get(pkg['status'])}")
        if not store.withdrawal_ready():
            body = (
                f'<p class=muted><a href="/projects/{_esc(pkg["project_id"])}">'
                "&larr; 返回项目</a></p>"
                '<div class=banner>存量包成员与序列引用关系补建尚未完成，'
                "撤回功能暂未开放。补建期间上传与下载正常，请稍后刷新本页。</div>"
            )
            return _page(f"撤回包 {package_id}", body, refresh=True)

        preview = store.preview_withdrawal(package_id)
        if "error" in preview:
            raise HTTPException(409, preview["error"])
        affected = preview["withdrawn"]
        rows = ""
        for pid, basis_list in affected.items():
            tag = "<strong>人工撤回入口</strong>" if pid == package_id else "连带撤回"
            basis_html = "<br>".join(_esc(b) for b in basis_list)
            rows += (
                "<tr>"
                f"<td class=muted>{_esc(pid)}</td><td>{tag}</td>"
                f'<td class=basis>{basis_html}</td></tr>'
            )
        body = f"""
        <p class=muted><a href="/projects/{_esc(pkg['project_id'])}">
        &larr; 返回项目</a></p>
        <div class=danger><strong>撤回为永久操作：</strong>受影响包将整包停供，
        拒绝新下载；后续任何上传只要包含或引用这些实例，整包拒收。
        原件保留，已在途下载不受影响。</div>
        <h2>受影响包（共 {len(affected)} 个，闭包已按"共享成员 / 引用撤回实例"
        两条规则递归算至不再扩大）</h2>
        <table><tr><th>包ID</th><th>角色</th><th>关联依据</th></tr>{rows}</table>
        <h2>填写撤回原因</h2>
        <form method=post action="/packages/{_esc(package_id)}/withdraw">
          <textarea name=reason maxlength=500 required
            placeholder="例如：该 CT 包系研究外误收，需永久停供"></textarea><br>
          <button type=submit
            onclick="return confirm('确认永久撤回这 {len(affected)} 个包？此操作不可恢复')">
            确认永久撤回（{len(affected)} 个包）</button>
        </form>"""
        return _page(f"撤回包 {package_id}", body)

    @app.post("/packages/{package_id}/withdraw", response_class=HTMLResponse)
    async def withdraw_submit(package_id: str, request: Request):
        form = await request.form()
        reason = str(form.get("reason") or "").strip()
        if not store.withdrawal_ready():
            raise HTTPException(409, "存量关系补建未完成，撤回暂未开放")
        result = store.withdraw_package(package_id, reason)
        if not result["ok"]:
            raise HTTPException(409, result["error"])
        rows = "".join(
            "<tr>"
            f"<td class=muted>{_esc(a['package_id'])}</td>"
            f"<td>{'人工撤回入口' if a['is_root'] else '连带撤回'}</td>"
            f"<td class=reason>{_esc(reason)}</td>"
            f'<td class=basis>{_esc(a["basis"])}</td></tr>'
            for a in result["affected"]
        )
        body = f"""
        <div class=okbox><strong>撤回成功：</strong>共 {len(result['affected'])}
        个包已整包永久停供，结论已落盘（重启保留）。新下载请求将被拒绝；
        后续上传包含或引用已撤回实例将整包拒收；原件保留。</div>
        <h2>本次受影响包与关联依据</h2>
        <table><tr><th>包ID</th><th>角色</th><th>撤回原因</th>
        <th>关联依据</th></tr>{rows}</table>
        <p><a href="/projects/{_esc(result['project_id'])}">&larr; 返回项目</a></p>"""
        return HTMLResponse(_page(f"撤回结果 {package_id}", body))

    # ---------------- JSON API ----------------
    @app.post("/api/projects")
    async def api_create_project(request: Request):
        payload = await request.json()
        name = str((payload or {}).get("name") or "").strip()
        if not name:
            raise HTTPException(400, "项目名称不能为空")
        project_id = store.create_project(name)
        return {"project_id": project_id, "name": name}

    @app.get("/api/projects/{project_id}")
    def api_project(project_id: str):
        project = store.get_project(project_id)
        if not project:
            raise HTTPException(404, "项目不存在")
        return {
            **project,
            "instance_count": store.instance_count(project_id),
            "withdrawal_ready": store.withdrawal_ready(),
            "backfill_pending": len(store.pending_backfill_package_ids()),
            "packages": store.list_packages(project_id),
            "withdrawals": store.list_withdrawals(project_id),
        }

    @app.post("/api/projects/{project_id}/packages")
    def api_upload(project_id: str, file: UploadFile = File(...)):
        package_id = _save_and_enqueue(project_id, file)
        return {"package_id": package_id}

    @app.get("/api/packages/{package_id}")
    def api_package(package_id: str):
        pkg = store.get_package(package_id)
        if not pkg:
            raise HTTPException(404, "包不存在")
        return pkg

    @app.get("/api/packages/{package_id}/withdraw/preview")
    def api_withdraw_preview(package_id: str):
        pkg = store.get_package(package_id)
        if not pkg:
            raise HTTPException(404, "包不存在")
        if not store.withdrawal_ready():
            raise HTTPException(409, "存量关系补建未完成，撤回暂未开放")
        preview = store.preview_withdrawal(package_id)
        if "error" in preview:
            raise HTTPException(409, preview["error"])
        return {
            "package_id": package_id,
            "project_id": preview["project_id"],
            "affected": [
                {"package_id": pid, "basis": basis}
                for pid, basis in preview["withdrawn"].items()
            ],
        }

    @app.post("/api/packages/{package_id}/withdraw")
    async def api_withdraw(package_id: str, request: Request):
        payload = await request.json()
        reason = str((payload or {}).get("reason") or "").strip()
        if not store.withdrawal_ready():
            raise HTTPException(409, "存量关系补建未完成，撤回暂未开放")
        result = store.withdraw_package(package_id, reason)
        if not result["ok"]:
            raise HTTPException(409, result["error"])
        return result

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "withdrawal_ready": store.withdrawal_ready()}

    # ---------------- 公共逻辑 ----------------
    def _save_and_enqueue(project_id: str, file: UploadFile) -> str:
        if not store.get_project(project_id):
            raise HTTPException(404, "项目不存在")
        package_id = store.create_package(project_id, file.filename or "upload.zip")
        dest = store.originals_dir / f"{package_id}.zip"
        size = 0
        try:
            with dest.open("wb") as f:
                while chunk := file.file.read(1 << 20):
                    size += len(chunk)
                    if size > pipeline.MAX_UPLOAD_BYTES:
                        dest.unlink(missing_ok=True)
                        store.set_status(
                            package_id, "rejected",
                            "上传文件超过 512MiB 大小限制，整包拒收")
                        raise HTTPException(413, "上传文件超过 512MiB 大小限制")
                    f.write(chunk)
        except HTTPException:
            dest.unlink(missing_ok=True)
            raise
        except Exception:
            dest.unlink(missing_ok=True)
            store.set_status(package_id, "failed", "上传保存失败")
            raise
        executor.submit(pipeline.process_package, store, package_id)
        return package_id

    return app


app = create_app()
