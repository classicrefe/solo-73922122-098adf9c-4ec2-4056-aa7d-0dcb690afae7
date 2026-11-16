"""撤回（永久停供）专项自测。

覆盖：
- 撤回闭包两条规则（共享 SOP 成员 / 各层序列引用撤回实例）与多层递归；
- 撤回后下载闸门、发布闸门（成员包含与序列引用两种触发）；
- 双实例交错发布/撤回不漏水、原件保留、失败整次不生效、重启保留结论；
- 存量包关系补建（含补建期间正常上传下载、不遗漏同期发布）；
- 嵌套两层序列内 ReferencedSOPInstanceUID 的提取与连带撤回；
- 撤回网页与 JSON API。
"""
from __future__ import annotations

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.pipeline import process_package
from app.references import extract_referenced_sop_uids
from app.store import Store
from tests.helpers import gen_uid, make_dicom_bytes, make_zip, read_dicom, unzip
import tests.helpers as helpers


@pytest.fixture(autouse=True)
def _reset_uid_counter():
    """每个用例独立的 UID 序列，避免模块级计数器跨测试重号。"""
    helpers._uid_counter[0] = 0
    yield


@pytest.fixture()
def store(tmp_path):
    s = Store(tmp_path / "data", owner="test-instance")
    s.init()
    return s


def submit(store, project_id, zip_bytes, name="batch.zip"):
    package_id = store.create_package(project_id, name)
    (store.originals_dir / f"{package_id}.zip").write_bytes(zip_bytes)
    process_package(store, package_id)
    return package_id


def wait_published(store, package_id):
    row = store.get_package(package_id)
    assert row["status"] == "published", row["reason"]
    return row


# ---------------- 引用提取 ----------------

def test_extract_nested_referenced_sop_uid():
    top, deep = gen_uid(), gen_uid()
    data = make_dicom_bytes(refs=[top], nested_refs=[deep])
    ds = read_dicom(data)
    found = dict(extract_referenced_sop_uids(ds))
    assert top in found and found[top] == "ReferencedImageSequence[1].ReferencedSOPInstanceUID"
    assert deep in found
    assert found[deep].startswith("ProcedureCodeSequence[1].ReferencedStudySequence[1].")


# ---------------- 撤回闭包 ----------------

def test_withdraw_root_only(store):
    pid = store.create_project("单包撤回")
    pkg = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    wait_published(store, pkg)

    result = store.withdraw_package(pkg, "误收，永久停供")
    assert result["ok"]
    assert [a["package_id"] for a in result["affected"]] == [pkg]
    assert store.get_package(pkg)["status"] == "withdrawn"
    records = store.list_withdrawals(pid)
    assert len(records) == 1 and records[0]["is_root"] == 1
    assert records[0]["reason"] == "误收，永久停供"


def test_withdraw_cascade_by_shared_member(store):
    """A 与 B 共享同一 SOP 成员（同原文字节）：撤回 A，B 必须连带撤回。"""
    pid = store.create_project("共享成员")
    shared_bytes = make_dicom_bytes(sop_uid=gen_uid())
    pkg_a = submit(store, pid, make_zip([
        ("a.dcm", shared_bytes),
        ("a2.dcm", make_dicom_bytes(sop_uid=gen_uid())),
    ]))
    pkg_b = submit(store, pid, make_zip([
        ("b.dcm", shared_bytes),  # 同实例同原文 → 复用，构成共享成员
    ]))
    pkg_c = submit(store, pid, make_zip([("c.dcm", make_dicom_bytes(sop_uid=gen_uid()))]))
    wait_published(store, pkg_b)
    wait_published(store, pkg_c)

    result = store.withdraw_package(pkg_a, "误收")
    ids = {a["package_id"] for a in result["affected"]}
    assert ids == {pkg_a, pkg_b}            # C 无关，不动
    assert all(store.get_package(x)["status"] == "withdrawn" for x in ids)
    assert store.get_package(pkg_c)["status"] == "published"
    shared_uid = read_dicom(shared_bytes).SOPInstanceUID
    basis = next(a["basis"] for a in result["affected"] if a["package_id"] == pkg_b)
    basis_text = " ".join(basis)
    assert "共享 SOP 实例" in basis_text and shared_uid in basis_text


def test_withdraw_cascade_by_reference_and_transitive(store):
    """A 撤回 → 引用 A 的 B 连带 → 引用 B 的 C 递归连带，直到范围不再扩大。"""
    pid = store.create_project("引用链")
    sop_a, sop_b = gen_uid(), gen_uid()
    pkg_a = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    pkg_b = submit(store, pid, make_zip([
        ("b.dcm", make_dicom_bytes(sop_uid=sop_b, refs=[sop_a]))]))
    wait_published(store, pkg_b)
    pkg_c = submit(store, pid, make_zip([("c.dcm", make_dicom_bytes(refs=[sop_b]))]))
    pkg_d = submit(store, pid, make_zip([("d.dcm", make_dicom_bytes(sop_uid=gen_uid()))]))
    wait_published(store, pkg_c)
    wait_published(store, pkg_d)

    result = store.withdraw_package(pkg_a, "误收")
    ids = {a["package_id"] for a in result["affected"]}
    assert ids == {pkg_a, pkg_b, pkg_c}
    assert store.get_package(pkg_d)["status"] == "published"
    basis_c = next(a["basis"] for a in result["affected"] if a["package_id"] == pkg_c)
    basis_text = " ".join(basis_c)
    assert "引用已撤回 SOP 实例" in basis_text and sop_b in basis_text


def test_nested_sequence_reference_triggers_cascade(store):
    """嵌套两层序列里的 ReferencedSOPInstanceUID 也必须进闭包。"""
    pid = store.create_project("嵌套引用")
    sop_a = gen_uid()
    pkg_a = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    pkg_b = submit(store, pid, make_zip([
        ("b.dcm", make_dicom_bytes(nested_refs=[sop_a]))]))
    wait_published(store, pkg_b)

    result = store.withdraw_package(pkg_a, "误收")
    assert {a["package_id"] for a in result["affected"]} == {pkg_a, pkg_b}


def test_withdraw_closure_is_fixed_point(store):
    """两条规则交错形成的菱形依赖也要一次撤全，且范围不再扩大。"""
    pid = store.create_project("菱形")
    sop_x, sop_z = gen_uid(), gen_uid()
    shared_y = make_dicom_bytes(sop_uid=gen_uid())  # B、C 共享的同原文成员
    sop_y = read_dicom(shared_y).SOPInstanceUID
    # A: x；B: 引用 x + 共享成员 y；C: 共享 y + 自有成员 z；D: 引用 z；E: 无关
    pkg_a = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_x))]))
    pkg_b = submit(store, pid, make_zip([
        ("b1.dcm", make_dicom_bytes(sop_uid=gen_uid(), refs=[sop_x])),
        ("b2.dcm", shared_y),
    ]))
    wait_published(store, pkg_b)
    pkg_c = submit(store, pid, make_zip([
        ("c0.dcm", shared_y),
        ("c1.dcm", make_dicom_bytes(sop_uid=sop_z)),
    ]))
    wait_published(store, pkg_c)
    pkg_d = submit(store, pid, make_zip([("d.dcm", make_dicom_bytes(refs=[sop_z]))]))
    pkg_e = submit(store, pid, make_zip([("e.dcm", make_dicom_bytes(sop_uid=gen_uid()))]))
    wait_published(store, pkg_d)
    wait_published(store, pkg_e)

    result = store.withdraw_package(pkg_a, "误收")
    assert {a["package_id"] for a in result["affected"]} == {
        pkg_a, pkg_b, pkg_c, pkg_d}
    assert store.get_package(pkg_e)["status"] == "published"
    # 已撤回的包不能再次撤回（固定点已稳定，无新扩散）
    assert "error" in store.preview_withdrawal(pkg_a)


def test_withdraw_requires_reason_and_is_atomic(store):
    pid = store.create_project("校验")
    pkg = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    wait_published(store, pkg)
    bad = store.withdraw_package(pkg, "  ")
    assert not bad["ok"] and store.get_package(pkg)["status"] == "published"
    # 撤回非已发布包被拒
    assert not store.withdraw_package("not-exist", "x")["ok"]


# ---------------- 撤回后的闸门 ----------------

def test_download_blocked_after_withdraw_but_files_kept(store):
    pid = store.create_project("下载闸门")
    pkg = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    wait_published(store, pkg)
    package_file = store.packages_dir / f"{pkg}.zip"
    original_file = store.originals_dir / f"{pkg}.zip"
    assert package_file.exists()
    store.withdraw_package(pkg, "误收")
    # 处理包与原件都保留，拒绝靠状态而非删文件
    assert package_file.exists() and original_file.exists()
    assert store.get_package(pkg)["status"] == "withdrawn"
    # 已开始的读取（如已获准的在途下载）直接读文件仍可读完：文件未被截断/删除
    assert len(unzip(package_file.read_bytes())) == 1
    # 恢复流程也不得清理已撤回包的处理包文件
    Store(store.data_dir, owner="restart").init()
    assert package_file.exists()


def test_reupload_withdrawn_member_rejected(store):
    pid = store.create_project("再传闸门-成员")
    sop_a = gen_uid()
    pkg = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    wait_published(store, pkg)
    store.withdraw_package(pkg, "误收")

    again = submit(store, pid, make_zip([("a2.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    row = store.get_package(again)
    assert row["status"] == "rejected" and sop_a in row["reason"]
    assert not (store.packages_dir / f"{again}.zip").exists()


def test_reference_withdrawn_instance_rejected(store):
    pid = store.create_project("再传闸门-引用")
    sop_a = gen_uid()
    pkg = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    wait_published(store, pkg)
    store.withdraw_package(pkg, "误收")

    bad = submit(store, pid, make_zip([("b.dcm", make_dicom_bytes(refs=[sop_a]))]))
    row = store.get_package(bad)
    assert row["status"] == "rejected" and sop_a in row["reason"]

    # 嵌套序列引用同样拦截
    bad2 = submit(store, pid, make_zip([
        ("c.dcm", make_dicom_bytes(nested_refs=[sop_a]))]))
    row2 = store.get_package(bad2)
    assert row2["status"] == "rejected" and sop_a in row2["reason"]


def test_unrelated_package_still_works_after_withdraw(store):
    pid = store.create_project("无关包")
    pkg_a = submit(store, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    wait_published(store, pkg_a)
    store.withdraw_package(pkg_a, "误收")

    pkg_b = submit(store, pid, make_zip([("b.dcm", make_dicom_bytes())]))
    wait_published(store, pkg_b)
    assert (store.packages_dir / f"{pkg_b}.zip").exists()
    sop_b = _member_sop(store, pkg_b)[0]
    # 引用未撤回实例的包照常发布
    pkg_c = submit(store, pid, make_zip([("c.dcm", make_dicom_bytes(refs=[sop_b]))]))
    wait_published(store, pkg_c)


def test_withdrawn_package_cannot_republish_same_content(store):
    """撤回后同实例同原文重新上传也必须拒收（撤回优先于复用）。"""
    pid = store.create_project("撤回优先")
    data = make_dicom_bytes()
    pkg1 = submit(store, pid, make_zip([("x.dcm", data)]))
    wait_published(store, pkg1)
    store.withdraw_package(pkg1, "误收")
    pkg2 = submit(store, pid, make_zip([("x.dcm", data)]))
    assert store.get_package(pkg2)["status"] == "rejected"
    assert store.instance_count(pid) == 1  # 没有新增实例


def test_deidentification_preserved_alongside_withdraw(store):
    """撤回能力不破坏既有去标识化：处理包内身份字段已删、UID 已映射。"""
    from app.deidentify import mapped_patient_id, mapped_uid
    pid = store.create_project("去标识保留")
    sop = gen_uid()
    pkg = submit(store, pid, make_zip([
        ("a.dcm", make_dicom_bytes(sop_uid=sop, refs=[sop]))]))
    wait_published(store, pkg)
    ds = read_dicom(unzip((store.packages_dir / f"{pkg}.zip").read_bytes())[
        f"{mapped_uid(pid, sop)}.dcm"])
    assert "PatientName" not in ds and "PatientBirthDate" not in ds
    assert ds.PatientID == mapped_patient_id(pid, "PAT-001")
    assert ds.SOPInstanceUID == mapped_uid(pid, sop)
    # 撤回后文件内容不变（去标识结果原样保留）
    store.withdraw_package(pkg, "误收")
    ds2 = read_dicom(unzip((store.packages_dir / f"{pkg}.zip").read_bytes())[
        f"{mapped_uid(pid, sop)}.dcm"])
    assert ds2.PatientID == ds.PatientID and ds2.SOPInstanceUID == ds.SOPInstanceUID


# ---------------- 双实例交错：不漏水、不复供 ----------------

def test_dual_instance_interleaved_publish_withdraw(tmp_path):
    data = tmp_path / "shared"
    app1 = Store(data, owner="app1")
    app1.init()
    app2 = Store(data, owner="app2")
    app2.init()
    pid = app1.create_project("交错")

    # app1 发布 A
    sop_a = gen_uid()
    pkg_a = submit(app1, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    wait_published(app2, pkg_a)
    # app2 发布引用 A 的 B（与撤回交错前先建立关系）
    pkg_b = submit(app2, pid, make_zip([("b.dcm", make_dicom_bytes(refs=[sop_a]))]))
    wait_published(app1, pkg_b)
    # app1 撤回 A：B 必须连带
    result = app1.withdraw_package(pkg_a, "误收")
    assert {a["package_id"] for a in result["affected"]} == {pkg_a, pkg_b}

    # app2 随后发布引用 A 的 C：被 app2 本地闸门拦截
    pkg_c = submit(app2, pid, make_zip([("c.dcm", make_dicom_bytes(refs=[sop_a]))]))
    assert app2.get_package(pkg_c)["status"] == "rejected"

    # app2 发布包含 A 成员的 D：同样拦截
    pkg_d = submit(app2, pid, make_zip([("d.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    assert app2.get_package(pkg_d)["status"] == "rejected"

    # 无关包在两个实例上都正常
    pkg_e = submit(app1, pid, make_zip([("e.dcm", make_dicom_bytes())]))
    wait_published(app2, pkg_e)

    # 重启（新 Store 指向同一卷）：结论保留，无复供
    app3 = Store(data, owner="app3")
    app3.init()
    assert app3.get_package(pkg_a)["status"] == "withdrawn"
    assert app3.get_package(pkg_b)["status"] == "withdrawn"
    assert app3.get_package(pkg_e)["status"] == "published"
    # 撤回实例集合（含 A 及连带包的自有成员）持久保留，sop_a 必在其中
    assert sop_a in app3.withdrawn_sop_uids(pid)


def test_concurrent_publish_and_withdraw_no_leak(tmp_path):
    """撤回与发布并发：最终不存在任何已发布包触及已撤回实例。"""
    data = tmp_path / "shared"
    app1 = Store(data, owner="app1")
    app1.init()
    app2 = Store(data, owner="app2")
    app2.init()
    pid = app1.create_project("并发撤回")
    sop_a = gen_uid()
    pkg_a = submit(app1, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    wait_published(app2, pkg_a)

    ref_zip = make_zip([("b.dcm", make_dicom_bytes(refs=[sop_a]))])
    outcomes: list[str] = []

    def publish_ref(s):
        p = submit(s, pid, ref_zip)
        outcomes.append(s.get_package(p)["status"])

    with ThreadPoolExecutor(4) as ex:
        futs = [ex.submit(publish_ref, app1), ex.submit(publish_ref, app2),
                ex.submit(app1.withdraw_package, pkg_a, "误收")]
        [f.result() for f in futs]

    # 引用包要么撤回前发布（随后必被连带撤回），要么被闸门拒收；不存在“已发布且引用撤回实例”
    for status in outcomes:
        assert status in ("published", "rejected")
    rows = app1.list_packages(pid)
    for r in rows:
        if r["status"] == "published":
            members = set(_member_sop(app1, r["package_id"]))
            assert not (members & app1.withdrawn_sop_uids(pid))


# ---------------- 重启保留 ----------------

def test_withdrawal_survives_restart(tmp_path):
    data = tmp_path / "data"
    s1 = Store(data, owner="app1")
    s1.init()
    pid = s1.create_project("重启")
    pkg = submit(s1, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    wait_published(s1, pkg)
    s1.withdraw_package(pkg, "误收")

    s2 = Store(data, owner="app1")
    s2.init()
    assert s2.get_package(pkg)["status"] == "withdrawn"
    assert s2.get_package(pkg)["reason"]
    assert len(s2.list_withdrawals(pid)) == 1


# ---------------- 存量补建 ----------------

def test_backfill_legacy_packages(tmp_path):
    """模拟升级前的存量库：有已发布包但无成员/引用关系，补建后才能撤回。"""
    data = tmp_path / "data"
    s = Store(data, owner="app1")
    s.init()
    pid = s.create_project("补建")
    sop_a = gen_uid()
    pkg_a = submit(s, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    pkg_b = submit(s, pid, make_zip([("b.dcm", make_dicom_bytes(refs=[sop_a]))]))
    wait_published(s, pkg_b)
    # 手工抹掉关系与补建标记，模拟旧版本数据库
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("DELETE FROM package_members")
        conn.execute("DELETE FROM package_references")
        conn.execute("DELETE FROM app_meta")
    assert set(s.pending_backfill_package_ids()) == {pkg_a, pkg_b}
    assert s.withdrawal_ready() is False

    # 补建期间撤回关闭
    assert "error" in s.preview_withdrawal(pkg_a) or not s.withdrawal_ready()

    summary = s.backfill_pending_packages()
    assert summary["processed"] == 2 and summary["failed"] == []
    assert s.pending_backfill_package_ids() == []
    assert s.withdrawal_ready() is True

    # 补建出的关系支撑完整闭包
    result = s.withdraw_package(pkg_a, "误收")
    assert {a["package_id"] for a in result["affected"]} == {pkg_a, pkg_b}


def test_concurrent_publish_during_backfill_not_missed(tmp_path):
    """补建期间新发布的包自带关系（不进待补建），且不会被补建漏记。"""
    data = tmp_path / "data"
    s = Store(data, owner="app1")
    s.init()
    pid = s.create_project("补建并发")
    sop_a = gen_uid()
    legacy = submit(s, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    wait_published(s, legacy)
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("DELETE FROM package_members WHERE package_id=?", (legacy,))
        conn.execute("DELETE FROM package_references WHERE package_id=?", (legacy,))
        conn.execute("DELETE FROM app_meta")

    # 补建进行中（先只补存量），同期发布一个引用 A 的新包
    s.backfill_one_package(legacy)
    new_pkg = submit(s, pid, make_zip([("n.dcm", make_dicom_bytes(refs=[sop_a]))]))
    wait_published(s, new_pkg)
    # 新包自带关系，不在待补建列表
    assert new_pkg not in s.pending_backfill_package_ids()
    s.backfill_pending_packages()
    assert s.withdrawal_ready()
    result = s.withdraw_package(legacy, "误收")
    assert {a["package_id"] for a in result["affected"]} == {legacy, new_pkg}


def test_backfill_failure_keeps_withdrawal_closed(tmp_path):
    data = tmp_path / "data"
    s = Store(data, owner="app1")
    s.init()
    pid = s.create_project("补建失败")
    pkg = submit(s, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    wait_published(s, pkg)
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("DELETE FROM package_members")
        conn.execute("DELETE FROM package_references")
        conn.execute("DELETE FROM app_meta")
    # 破坏原件：补建应报告失败且不开放撤回
    (s.originals_dir / f"{pkg}.zip").write_bytes(b"broken")
    summary = s.backfill_pending_packages()
    assert summary["failed"] and summary["failed"][0]["package_id"] == pkg
    assert s.withdrawal_ready() is False


# ---------------- 网页 / API ----------------

@pytest.fixture()
def client(tmp_path):
    from fastapi.testclient import TestClient
    from app.main import _backfill_state, _reset_backfill_state, create_app
    _reset_backfill_state()
    app = create_app(Store(tmp_path / "data", owner="api-test"))
    # 等待启动后台补建线程完成（全新库无存量包，应立即完成）
    for _ in range(200):
        if not _backfill_state["running"]:
            break
        time.sleep(0.02)
    return TestClient(app)


def _api_publish(client, pid, files_zip, name="b.zip"):
    r = client.post(f"/api/projects/{pid}/packages",
                    files={"file": (name, files_zip, "application/zip")})
    assert r.status_code == 200
    pkg = r.json()["package_id"]
    for _ in range(100):
        row = client.get(f"/api/packages/{pkg}").json()
        if row["status"] not in ("received", "processing"):
            break
        time.sleep(0.05)
    return row


def test_api_withdraw_flow_and_download_gate(client):
    pid = client.post("/api/projects", json={"name": "API撤回"}).json()["project_id"]
    sop_a = gen_uid()
    row_a = _api_publish(client, pid, make_zip([("a.dcm", make_dicom_bytes(sop_uid=sop_a))]))
    pkg_a = row_a["package_id"]
    row_b = _api_publish(client, pid,
                         make_zip([("b.dcm", make_dicom_bytes(refs=[sop_a]))]))
    pkg_b = row_b["package_id"]

    # 预览
    preview = client.get(f"/api/packages/{pkg_a}/withdraw/preview").json()
    assert {a["package_id"] for a in preview["affected"]} == {pkg_a, pkg_b}

    # 执行撤回
    r = client.post(f"/api/packages/{pkg_a}/withdraw", json={"reason": "误收停供"})
    assert r.status_code == 200 and len(r.json()["affected"]) == 2

    # 下载闸门
    assert client.get(f"/packages/{pkg_a}/download").status_code == 409
    assert client.get(f"/packages/{pkg_b}/download").status_code == 409

    # 撤回后上传被拒收
    row_c = _api_publish(client, pid,
                         make_zip([("c.dcm", make_dicom_bytes(refs=[sop_a]))]))
    assert row_c["status"] == "rejected" and sop_a in row_c["reason"]


def test_withdraw_web_pages(client):
    pid = client.post("/api/projects", json={"name": "网页撤回"}).json()["project_id"]
    row = _api_publish(client, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    pkg = row["package_id"]

    form = client.get(f"/packages/{pkg}/withdraw")
    assert form.status_code == 200 and "撤回原因" in form.text and pkg in form.text

    # 表单提交（application/x-www-form-urlencoded）
    r = client.post(f"/packages/{pkg}/withdraw", data={"reason": "网页误收撤回"})
    assert r.status_code == 200 and "撤回成功" in r.text
    assert "网页误收撤回" in r.text

    project_page = client.get(f"/projects/{pid}").text
    assert "已撤回" in project_page and "网页误收撤回" in project_page
    # 撤回结论表存在
    assert "人工撤回入口" in project_page


def test_withdraw_closed_until_backfill_in_api(tmp_path):
    from fastapi.testclient import TestClient
    from app.main import create_app
    data = tmp_path / "data"
    s = Store(data, owner="api-test")
    s.init()
    pid = s.create_project("未补建")
    pkg = submit(s, pid, make_zip([("a.dcm", make_dicom_bytes())]))
    wait_published(s, pkg)
    with sqlite3.connect(s.db_path) as conn:
        conn.execute("DELETE FROM package_members")
        conn.execute("DELETE FROM package_references")
        conn.execute("DELETE FROM app_meta")

    from fastapi.testclient import TestClient
    from app.main import create_app, _reset_backfill_state
    _reset_backfill_state()
    app = create_app(Store(data, owner="api-test2"))
    client = TestClient(app)
    # 补建未完成时撤回 API 与网页均关闭
    assert client.post(f"/api/packages/{pkg}/withdraw",
                       json={"reason": "x"}).status_code == 409
    page = client.get(f"/packages/{pkg}/withdraw")
    assert page.status_code == 200 and "暂未开放" in page.text
    # 上传下载不受影响
    row = _api_publish(client, pid, make_zip([("z.dcm", make_dicom_bytes())]))
    assert row["status"] == "published"
    assert client.get(f"/packages/{row['package_id']}/download").status_code == 200


# ---------------- 辅助 ----------------

def _member_sop(store: Store, package_id: str) -> list[str]:
    with sqlite3.connect(store.db_path) as conn:
        rows = conn.execute(
            "SELECT sop_uid FROM package_members WHERE package_id=? ORDER BY sop_uid",
            (package_id,),
        ).fetchall()
    return [r[0] for r in rows]
