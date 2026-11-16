"""共享卷目录 + SQLite 目录库。

设计要点：
- 原件、处理包存共享卷（originals/ packages/ staging/），SQLite 只存目录元数据。
- WAL 模式 + busy_timeout：转换期间其他上传/下载（读）不被阻塞。
- 发布用 BEGIN IMMEDIATE 事务：双服务实例并发上传时串行化，
  同事务内完成"冲突检查 + 撤回闸门 + 实例登记 + 成员/引用关系登记 +
  包状态更新"，杜绝未发布实例被引用，也杜绝撤回后漏判。
- 撤回在单事务内做固定点闭包（共享 SOP 成员 / 引用已撤回实例两条规则），
  结论写入 withdrawals 表，重启后仍然有效。
- 启动恢复：中断的包标记失败，清理 staging 与无主包文件；已撤回包的文件保留。
- 存量包补建：后台解析原件 ZIP，补齐 package_members/package_references，
  补建完成前不开放撤回。
"""
from __future__ import annotations

import io
import logging
import os
import socket
import sqlite3
import time
import uuid
import zipfile
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pydicom

from .references import extract_referenced_sop_uids

log = logging.getLogger("deid.store")

SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    project_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS packages (
    package_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id),
    status TEXT NOT NULL,              -- received/processing/published/rejected/failed/withdrawn
    reason TEXT,
    original_name TEXT,
    original_path TEXT,
    package_path TEXT,
    instance_count INTEGER NOT NULL DEFAULT 0,
    owner TEXT,                        -- 接收该包的包的服务实例标识
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS instances (
    project_id TEXT NOT NULL,
    sop_uid TEXT NOT NULL,             -- 原始 SOPInstanceUID
    sha256 TEXT NOT NULL,              -- 原始文件字节哈希（同实例判重用）
    study_uid TEXT NOT NULL,           -- 原始 StudyInstanceUID
    series_uid TEXT NOT NULL,          -- 原始 SeriesInstanceUID
    mapped_sop_uid TEXT NOT NULL,      -- 替换后的 SOPInstanceUID
    package_id TEXT NOT NULL,
    published_at TEXT NOT NULL,
    PRIMARY KEY (project_id, sop_uid)
);
CREATE TABLE IF NOT EXISTS package_members (
    project_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    sop_uid TEXT NOT NULL,             -- 成员实例的原始 SOPInstanceUID
    PRIMARY KEY (package_id, sop_uid)
);
CREATE INDEX IF NOT EXISTS idx_members_sop ON package_members(project_id, sop_uid);
CREATE TABLE IF NOT EXISTS package_references (
    project_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    referenced_sop_uid TEXT NOT NULL,  -- 被引用实例的原始 SOPInstanceUID
    ref_path TEXT NOT NULL,            -- 引用所在元素路径（关联依据）
    PRIMARY KEY (package_id, referenced_sop_uid, ref_path)
);
CREATE INDEX IF NOT EXISTS idx_refs_uid
    ON package_references(project_id, referenced_sop_uid);
CREATE TABLE IF NOT EXISTS withdrawals (
    package_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    root_package_id TEXT NOT NULL,     -- 触发本次撤回的人工撤回包
    reason TEXT NOT NULL,              -- 人工填写的撤回原因（连带包沿用）
    basis TEXT NOT NULL,               -- 关联依据（多条以 " | " 连接）
    is_root INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_withdrawals_project ON withdrawals(project_id);
CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_packages_project ON packages(project_id);
"""

STATUS_LABELS = {
    "received": "已接收",
    "processing": "处理中",
    "published": "已发布",
    "rejected": "已拒收",
    "failed": "失败",
    "withdrawn": "已撤回",
}

# 启动恢复判定“死亡”在途包的时间阈值：处理 1~20 份单帧实例只需秒级，
# updated_at 超过该阈值的在途包必然属于已死实例。
STALE_PACKAGE_SECONDS = 600
# 清理无主文件的安全宽限：另一存活实例可能刚写入/改名，只清理更老的文件。
ORPHAN_FILE_GRACE_SECONDS = 60

BACKFILL_DONE_KEY = "backfill_complete"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _zip_candidate_names(zf: zipfile.ZipFile) -> list[str]:
    names = []
    for m in zf.infolist():
        name = m.filename
        if m.is_dir() or name.startswith("__MACOSX/"):
            continue
        base = name.rsplit("/", 1)[-1]
        if base and not base.startswith("."):
            names.append(name)
    return names


class Store:
    """目录库与共享卷路径。所有方法均可跨线程、跨进程安全调用。"""

    def __init__(self, data_dir: str | os.PathLike, owner: str | None = None):
        self.data_dir = Path(data_dir)
        self.originals_dir = self.data_dir / "originals"
        self.packages_dir = self.data_dir / "packages"
        self.staging_dir = self.data_dir / "staging"
        self.db_path = self.data_dir / "catalog.db"
        self.owner = owner or socket.gethostname()

    # ---------------- 初始化与恢复 ----------------
    def init(self) -> None:
        for d in (self.originals_dir, self.packages_dir, self.staging_dir):
            d.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            with conn:
                conn.executescript(SCHEMA)
                conn.execute("PRAGMA journal_mode=WAL")
        self.recover()

    def recover(self) -> None:
        """启动恢复（不干扰另一存活实例）：

        - 本实例遗留的、以及超时停滞的在途包标记为失败；
        - staging/ 中超过宽限期的文件一律清理（宽限期内的可能正被另一实例写入）；
        - 清理无发布/撤回记录且超过宽限期的 packages/ 孤儿文件（可能来自
          “已改名未提交”的崩溃窗口）；宽限期内的文件属于另一实例的在途发布。
          已撤回包的处理包文件必须保留（下载由状态闸门拒绝，文件不删）。
        """
        now = _now()
        stale_before = (
            datetime.now(timezone.utc) - timedelta(seconds=STALE_PACKAGE_SECONDS)
        ).isoformat(timespec="seconds")
        with closing(self._connect()) as conn:
            with conn:
                conn.execute(
                    "UPDATE packages SET status='failed', reason=?, updated_at=? "
                    "WHERE status IN ('received','processing') AND owner=?",
                    ("服务重启导致处理中断，请重新上传", now, self.owner),
                )
                conn.execute(
                    "UPDATE packages SET status='failed', reason=?, updated_at=? "
                    "WHERE status IN ('received','processing') AND updated_at < ?",
                    ("处理超时停滞（实例可能已宕机），请重新上传", now, stale_before),
                )
                rows = conn.execute(
                    "SELECT package_id FROM packages "
                    "WHERE status IN ('published','withdrawn')"
                ).fetchall()
        kept = {row["package_id"] for row in rows}
        cutoff = time.time() - ORPHAN_FILE_GRACE_SECONDS
        # staging 是暂存区：只清理超过宽限期的文件，宽限期内的可能正被另一实例写入
        for f in self.staging_dir.glob("*.zip"):
            if f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
        for f in self.packages_dir.glob("*.zip"):
            if f.stem not in kept and f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    # ---------------- 项目 ----------------
    def create_project(self, name: str) -> str:
        project_id = "P" + uuid.uuid4().hex[:10].upper()
        with closing(self._connect()) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO projects (project_id, name, created_at) VALUES (?,?,?)",
                    (project_id, name, _now()),
                )
        return project_id

    def get_project(self, project_id: str) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM projects WHERE project_id=?", (project_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_projects(self) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT p.*,
                    (SELECT COUNT(*) FROM packages k
                      WHERE k.project_id = p.project_id) AS package_count,
                    (SELECT COUNT(*) FROM instances i
                      WHERE i.project_id = p.project_id) AS instance_count
                FROM projects p ORDER BY p.created_at DESC
                """
            ).fetchall()
        return [dict(r) for r in rows]

    # ---------------- 包 ----------------
    def create_package(self, project_id: str, original_name: str) -> str:
        package_id = uuid.uuid4().hex[:16]
        now = _now()
        with closing(self._connect()) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO packages (package_id, project_id, status, reason,"
                    " original_name, original_path, package_path, instance_count,"
                    " owner, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        package_id, project_id, "received", None, original_name,
                        f"originals/{package_id}.zip", None, 0, self.owner, now, now,
                    ),
                )
        return package_id

    def get_package(self, package_id: str) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM packages WHERE package_id=?", (package_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_packages(self, project_id: str) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM packages WHERE project_id=? ORDER BY created_at DESC",
                (project_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def set_status(
        self,
        package_id: str,
        status: str,
        reason: str | None = None,
        package_path: str | None = None,
        instance_count: int | None = None,
    ) -> None:
        with closing(self._connect()) as conn:
            with conn:
                conn.execute(
                    "UPDATE packages SET status=?, reason=?,"
                    " package_path=COALESCE(?, package_path),"
                    " instance_count=COALESCE(?, instance_count), updated_at=?"
                    " WHERE package_id=?",
                    (status, reason, package_path, instance_count, _now(), package_id),
                )

    # ---------------- 实例目录 ----------------
    def published_uids(self, project_id: str) -> set[str]:
        """本项目已发布实例的全部原始 UID（sop/study/series），用于引用校验。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT sop_uid, study_uid, series_uid FROM instances WHERE project_id=?",
                (project_id,),
            ).fetchall()
        uids: set[str] = set()
        for r in rows:
            uids.update((r["sop_uid"], r["study_uid"], r["series_uid"]))
        return uids

    def instance_count(self, project_id: str) -> int:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM instances WHERE project_id=?", (project_id,)
            ).fetchone()
        return int(row["n"])

    def withdrawn_sop_uids(self, project_id: str) -> set[str]:
        """本项目已撤回包的全部成员原始 SOPInstanceUID（发布闸门用）。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT DISTINCT m.sop_uid FROM package_members m "
                "JOIN packages k ON k.package_id = m.package_id "
                "WHERE m.project_id=? AND k.status='withdrawn'",
                (project_id,),
            ).fetchall()
        return {r["sop_uid"] for r in rows}

    def publish_package(
        self,
        package_id: str,
        project_id: str,
        instances: list[dict],
        package_path: str,
        references: list[tuple[str, str]],
    ) -> dict:
        """事务发布：冲突检查 + 撤回闸门 + 实例登记 + 成员/引用关系登记
        + 包状态更新，全部原子完成。

        返回：
          {"ok": True}
          {"ok": False, "kind": "conflict", "uid": ...}   同实例异文
          {"ok": False, "kind": "withdrawn", "uid": ..., "ref": bool}
              包成员包含 / 序列引用了已撤回实例
        """
        now = _now()
        member_uids = [inst["sop_uid"] for inst in instances]
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            # 1) 同实例异文冲突检查
            for inst in instances:
                row = conn.execute(
                    "SELECT sha256 FROM instances WHERE project_id=? AND sop_uid=?",
                    (project_id, inst["sop_uid"]),
                ).fetchone()
                if row and row["sha256"] != inst["sha256"]:
                    conn.rollback()
                    return {"ok": False, "kind": "conflict", "uid": inst["sop_uid"]}

            # 2) 撤回闸门：成员包含或任意层序列引用已撤回实例 → 整包拒收
            withdrawn_rows = conn.execute(
                "SELECT DISTINCT m.sop_uid FROM package_members m "
                "JOIN packages k ON k.package_id = m.package_id "
                "WHERE m.project_id=? AND k.status='withdrawn'",
                (project_id,),
            ).fetchall()
            withdrawn_uids = {r["sop_uid"] for r in withdrawn_rows}
            if withdrawn_uids:
                for uid in member_uids:
                    if uid in withdrawn_uids:
                        conn.rollback()
                        return {"ok": False, "kind": "withdrawn", "uid": uid,
                                "ref": False}
                for ref_uid, _path in references:
                    if ref_uid in withdrawn_uids:
                        conn.rollback()
                        return {"ok": False, "kind": "withdrawn", "uid": ref_uid,
                                "ref": True}

            # 3) 实例登记（同实例同原文 INSERT OR IGNORE 复用）
            for inst in instances:
                conn.execute(
                    "INSERT OR IGNORE INTO instances (project_id, sop_uid, sha256,"
                    " study_uid, series_uid, mapped_sop_uid, package_id, published_at)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (
                        project_id, inst["sop_uid"], inst["sha256"], inst["study_uid"],
                        inst["series_uid"], inst["mapped_sop_uid"], package_id, now,
                    ),
                )
            # 4) 成员关系登记
            for uid in member_uids:
                conn.execute(
                    "INSERT OR IGNORE INTO package_members (project_id, package_id,"
                    " sop_uid) VALUES (?,?,?)",
                    (project_id, package_id, uid),
                )
            # 5) 各层序列引用关系登记
            for ref_uid, ref_path in references:
                conn.execute(
                    "INSERT OR IGNORE INTO package_references (project_id, package_id,"
                    " referenced_sop_uid, ref_path) VALUES (?,?,?,?)",
                    (project_id, package_id, ref_uid, ref_path),
                )
            # 6) 包状态更新
            conn.execute(
                "UPDATE packages SET status='published', reason=NULL, package_path=?,"
                " instance_count=?, updated_at=? WHERE package_id=?",
                (package_path, len(instances), now, package_id),
            )
            conn.commit()
            return {"ok": True}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------------- 存量包关系补建 ----------------
    def get_meta(self, key: str) -> str | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT value FROM app_meta WHERE key=?", (key,)
            ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with closing(self._connect()) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO app_meta (key, value) VALUES (?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, value),
                )

    def pending_backfill_package_ids(self) -> list[str]:
        """已发布但尚无成员关系记录的包（含补建失败待重试的包）。"""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT k.package_id FROM packages k WHERE k.status='published' "
                "AND NOT EXISTS (SELECT 1 FROM package_members m "
                "                WHERE m.package_id = k.package_id) "
                "ORDER BY k.created_at"
            ).fetchall()
        return [r["package_id"] for r in rows]

    def backfill_one_package(self, package_id: str) -> None:
        """解析原件 ZIP 补齐单个包的成员与各层序列引用关系。"""
        pkg = self.get_package(package_id)
        if not pkg:
            return
        project_id = pkg["project_id"]
        original_zip = self.originals_dir / f"{package_id}.zip"
        members: list[str] = []
        references: list[tuple[str, str]] = []
        with zipfile.ZipFile(original_zip) as zf:
            for name in _zip_candidate_names(zf):
                ds = pydicom.dcmread(io.BytesIO(zf.read(name)))
                sop_uid = str(ds.SOPInstanceUID)
                members.append(sop_uid)
                for ref_uid, ref_path in extract_referenced_sop_uids(ds):
                    references.append((ref_uid, f"{name}::{ref_path}"))
        with closing(self._connect()) as conn:
            with conn:
                for uid in members:
                    conn.execute(
                        "INSERT OR IGNORE INTO package_members (project_id, package_id,"
                        " sop_uid) VALUES (?,?,?)",
                        (project_id, package_id, uid),
                    )
                for ref_uid, ref_path in references:
                    conn.execute(
                        "INSERT OR IGNORE INTO package_references (project_id, package_id,"
                        " referenced_sop_uid, ref_path) VALUES (?,?,?,?)",
                        (project_id, package_id, ref_uid, ref_path),
                    )

    def backfill_pending_packages(self) -> dict:
        """补齐所有缺关系的已发布存量包。失败包跳过并统计，不中断整体补建。"""
        processed, failed = 0, []
        for package_id in self.pending_backfill_package_ids():
            try:
                self.backfill_one_package(package_id)
                processed += 1
            except Exception as exc:  # 原件损坏等：保留待补建状态，撤回继续关闭
                log.warning("backfill failed for package %s: %s", package_id, exc)
                failed.append({"package_id": package_id, "error": str(exc)})
        # 无待补建包即视为补建完成（全新库也会落此标记，开放撤回）
        if not self.pending_backfill_package_ids():
            self.set_meta(BACKFILL_DONE_KEY, _now())
        return {"processed": processed, "failed": failed}

    def withdrawal_ready(self) -> bool:
        """关系补建全部完成且无待补建包时才开放撤回。"""
        return self.get_meta(BACKFILL_DONE_KEY) is not None \
            and not self.pending_backfill_package_ids()

    # ---------------- 撤回闭包 ----------------
    def _compute_closure(self, conn, root_package_id: str) -> dict | dict:
        """固定点闭包：从 root 出发，按两条规则扩展直至范围不再扩大。

        规则1：与任一已撤回包共享 SOP 成员实例；
        规则2：任意层序列引用了任一已撤回包的成员实例。
        仅波及 status='published' 的包；已撤回/失败/拒收包不受影响。
        返回 {package_id: [依据, ...]}，root 首条依据为人工撤回。
        """
        root = conn.execute(
            "SELECT * FROM packages WHERE package_id=?", (root_package_id,)
        ).fetchone()
        if not root:
            return {"error": "包不存在"}
        if root["status"] != "published":
            return {"error": f"包当前状态为 {STATUS_LABELS.get(root['status'])}，"
                             "仅已发布包可以撤回"}
        project_id = root["project_id"]
        withdrawn: dict[str, list[str]] = {
            root_package_id: ["人工撤回（撤回入口包）"]
        }
        frontier = [root_package_id]
        while frontier:
            # 当前前沿包的成员 UID -> 所属已撤回包（用于规则1的依据描述）
            uid_owners: dict[str, list[str]] = {}
            placeholders = ",".join("?" for _ in frontier)
            owner_rows = conn.execute(
                f"SELECT package_id, sop_uid FROM package_members "
                f"WHERE package_id IN ({placeholders})",
                frontier,
            ).fetchall()
            for r in owner_rows:
                uid_owners.setdefault(r["sop_uid"], []).append(r["package_id"])
            frontier_uids = list(uid_owners)
            newly: dict[str, list[str]] = {}

            if frontier_uids:
                ph = ",".join("?" for _ in frontier_uids)
                # 规则1：共享 SOP 成员实例
                shared_rows = conn.execute(
                    f"SELECT DISTINCT m.package_id, m.sop_uid FROM package_members m "
                    f"JOIN packages k ON k.package_id = m.package_id "
                    f"WHERE m.project_id=? AND k.status='published' "
                    f"AND m.sop_uid IN ({ph})",
                    [project_id, *frontier_uids],
                ).fetchall()
                for r in shared_rows:
                    pid = r["package_id"]
                    if pid in withdrawn or pid in newly:
                        continue
                    src = "、".join(uid_owners[r["sop_uid"]])
                    newly[pid] = [
                        f"与已撤回包 {src} 共享 SOP 实例 {r['sop_uid']}（成员重叠）"
                    ]

                # 规则2：任意层序列引用了已撤回实例（累计全部已撤回包的成员）
                all_withdrawn_ids = list(withdrawn) + list(newly)
                ph2 = ",".join("?" for _ in all_withdrawn_ids)
                member_rows = conn.execute(
                    f"SELECT DISTINCT sop_uid FROM package_members "
                    f"WHERE package_id IN ({ph2})",
                    all_withdrawn_ids,
                ).fetchall()
                all_withdrawn_uids = [r["sop_uid"] for r in member_rows]
                if all_withdrawn_uids:
                    ph3 = ",".join("?" for _ in all_withdrawn_uids)
                    ref_rows = conn.execute(
                        f"SELECT r.package_id, r.referenced_sop_uid, r.ref_path "
                        f"FROM package_references r "
                        f"JOIN packages k ON k.package_id = r.package_id "
                        f"WHERE r.project_id=? AND k.status='published' "
                        f"AND r.referenced_sop_uid IN ({ph3})",
                        [project_id, *all_withdrawn_uids],
                    ).fetchall()
                    for r in ref_rows:
                        pid = r["package_id"]
                        if pid in withdrawn or pid in newly:
                            continue
                        newly[pid] = [
                            f"在 {r['ref_path']} 引用已撤回 SOP 实例 "
                            f"{r['referenced_sop_uid']}"
                        ]
            for pid, basis in newly.items():
                withdrawn[pid] = basis
            frontier = list(newly)
        return {"project_id": project_id, "withdrawn": withdrawn}

    def preview_withdrawal(self, package_id: str) -> dict:
        """只读预览撤回闭包（撤回网页展示受影响包与关联依据）。"""
        with closing(self._connect()) as conn:
            return self._compute_closure(conn, package_id)

    def withdraw_package(self, root_package_id: str, reason: str) -> dict:
        """执行撤回：单事务固定点闭包 + 整包状态更新 + 结论落盘。

        失败整次不生效（事务回滚，无任何包状态变化）。
        """
        reason = (reason or "").strip()
        if not reason:
            return {"ok": False, "error": "撤回原因不能为空"}
        if len(reason) > 500:
            return {"ok": False, "error": "撤回原因不能超过 500 字"}
        now = _now()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = self._compute_closure(conn, root_package_id)
            if "error" in result:
                conn.rollback()
                return {"ok": False, "error": result["error"]}
            project_id = result["project_id"]
            withdrawn: dict[str, list[str]] = result["withdrawn"]
            for pid, basis_list in withdrawn.items():
                is_root = pid == root_package_id
                if is_root:
                    pkg_reason = reason
                    basis = " | ".join(basis_list) + f"；原因：{reason}"
                else:
                    pkg_reason = (
                        f"连带撤回（源头包 {root_package_id}）：{reason}"
                    )
                    basis = " | ".join(basis_list)
                conn.execute(
                    "UPDATE packages SET status='withdrawn', reason=?, updated_at=? "
                    "WHERE package_id=? AND status='published'",
                    (pkg_reason, now, pid),
                )
                conn.execute(
                    "INSERT INTO withdrawals (package_id, project_id, root_package_id,"
                    " reason, basis, is_root, created_at) VALUES (?,?,?,?,?,?,?) "
                    "ON CONFLICT(package_id) DO UPDATE SET reason=excluded.reason,"
                    " basis=excluded.basis, root_package_id=excluded.root_package_id,"
                    " created_at=excluded.created_at",
                    (pid, project_id, root_package_id, reason, basis,
                     1 if is_root else 0, now),
                )
            conn.commit()
            affected = [
                {
                    "package_id": pid,
                    "is_root": pid == root_package_id,
                    "basis": basis,
                }
                for pid, basis in withdrawn.items()
            ]
            log.info("withdraw root=%s affected=%d", root_package_id, len(affected))
            return {"ok": True, "project_id": project_id, "affected": affected}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_withdrawals(self, project_id: str) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM withdrawals WHERE project_id=? ORDER BY created_at DESC",
                (project_id,),
            ).fetchall()
        return [dict(r) for r in rows]
