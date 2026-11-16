#!/usr/bin/env python3
"""撤回（永久停供）双实例端到端演示脚本。

前提：docker compose up（或本地启动两个实例），默认访问
  app1 = http://localhost:8001, app2 = http://localhost:8002

演示内容：
  1. app1 创建项目；批次1（实例 A、B）经 app1 发布；
  2. 批次2 与批次1 共享实例 A（同原文，跨实例复用）经 app2 发布；
  3. 批次3 引用实例 A 经 app1 发布；
  4. 无关批次4 经 app2 发布（撤回后应照常可用）；
  5. 在 app1 对批次1 发起撤回预览，确认闭包波及批次1/2/3，不含批次4；
  6. 执行撤回：三个包整包停供，网页/API 下载均被拒（409）；
  7. app2 重新上传包含 A 的包 → 整包拒收；上传引用 A 的包 → 整包拒收；
  8. 无关包在两个实例上仍可上传下载；重启后结论保留（由持久化保证）。
"""
from __future__ import annotations

import argparse
import io
import sys
import time
import uuid
import zipfile

import httpx
import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian

CT_IMAGE_STORAGE = "1.2.840.10008.5.1.4.1.1.2"
_counter = [0]


def gen_uid() -> str:
    _counter[0] += 1
    return f"1.2.826.0.1.3680043.9.7433.{int(time.time())}.{_counter[0]}"


def mapped_uid(project_id: str, original_uid: str) -> str:
    return "2.25." + str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"deid:{project_id}:{original_uid}").int)


def make_dicom(*, sop_uid=None, refs=(), patient_name="Zhang^San") -> bytes:
    sop_uid = sop_uid or gen_uid()
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = CT_IMAGE_STORAGE
    meta.MediaStorageSOPInstanceUID = sop_uid
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = Dataset()
    ds.file_meta = meta
    ds.SOPClassUID = CT_IMAGE_STORAGE
    ds.SOPInstanceUID = sop_uid
    ds.StudyInstanceUID = gen_uid()
    ds.SeriesInstanceUID = gen_uid()
    ds.PatientID = "PAT-001"
    ds.PatientName = patient_name
    ds.PatientBirthDate = "19700101"
    ds.Modality = "CT"
    ds.BurnedInAnnotation = "NO"
    ds.Rows = ds.Columns = 2
    ds.BitsAllocated = ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 0
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    ds.PixelData = b"\x01\x02\x03\x04\x05\x06\x07\x08"
    if refs:
        ds.ReferencedImageSequence = []
        for r in refs:
            item = Dataset()
            item.ReferencedSOPClassUID = CT_IMAGE_STORAGE
            item.ReferencedSOPInstanceUID = r
            ds.ReferencedImageSequence.append(item)
    buf = io.BytesIO()
    pydicom.dcmwrite(buf, ds, enforce_file_format=True)
    return buf.getvalue()


def make_zip(files: list[tuple[str, bytes]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files:
            zf.writestr(name, data)
    return buf.getvalue()


def upload(client: httpx.Client, base: str, project_id: str,
           zip_bytes: bytes, filename: str) -> str:
    r = client.post(f"{base}/api/projects/{project_id}/packages",
                    files={"file": (filename, zip_bytes, "application/zip")})
    r.raise_for_status()
    return r.json()["package_id"]


def wait_done(client: httpx.Client, base: str, package_id: str) -> dict:
    for _ in range(120):
        row = client.get(f"{base}/api/packages/{package_id}").json()
        if row["status"] not in ("received", "processing"):
            return row
        time.sleep(0.5)
    raise TimeoutError(f"包 {package_id} 处理超时")


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" —— {detail}" if detail else ""))
    return ok


def wait_ready(client: httpx.Client, base: str) -> None:
    for _ in range(100):
        if client.get(f"{base}/healthz").json().get("withdrawal_ready"):
            return
        time.sleep(0.3)
    raise TimeoutError("关系补建迟迟未完成")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--app1", default="http://localhost:8001")
    ap.add_argument("--app2", default="http://localhost:8002")
    args = ap.parse_args()

    results: list[bool] = []
    with httpx.Client(timeout=30) as client:
        pid = client.post(f"{args.app1}/api/projects",
                          json={"name": "撤回演示"}).json()["project_id"]
        print(f"项目 {pid} 已创建（经由 app1），等待存量关系补建完成…")
        wait_ready(client, args.app1)

        # 1) 批次1：A + B（app1）
        sop_a, sop_b = gen_uid(), gen_uid()
        dcm_a = make_dicom(sop_uid=sop_a)
        batch1 = make_zip([("a.dcm", dcm_a), ("b.dcm", make_dicom(sop_uid=sop_b))])
        r1 = wait_done(client, args.app1,
                       upload(client, args.app1, pid, batch1, "batch1.zip"))
        results.append(check("批次1（A、B）经 app1 发布",
                             r1["status"] == "published", r1.get("reason") or ""))
        pkg1 = r1["package_id"]

        # 2) 批次2：与批次1 共享 A（同原文复用，app2）
        batch2 = make_zip([("a-copy.dcm", dcm_a),
                           ("c.dcm", make_dicom())])
        r2 = wait_done(client, args.app2,
                       upload(client, args.app2, pid, batch2, "batch2.zip"))
        results.append(check("批次2 共享实例 A 经 app2 发布（复用）",
                             r2["status"] == "published", r2.get("reason") or ""))
        pkg2 = r2["package_id"]

        # 3) 批次3：引用 A（app1）
        batch3 = make_zip([("d.dcm", make_dicom(refs=[sop_a]))])
        r3 = wait_done(client, args.app1,
                       upload(client, args.app1, pid, batch3, "batch3.zip"))
        results.append(check("批次3 引用实例 A 经 app1 发布",
                             r3["status"] == "published", r3.get("reason") or ""))
        pkg3 = r3["package_id"]

        # 4) 无关批次4（app2）
        batch4 = make_zip([("e.dcm", make_dicom())])
        r4 = wait_done(client, args.app2,
                       upload(client, args.app2, pid, batch4, "batch4.zip"))
        results.append(check("无关批次4 经 app2 发布", r4["status"] == "published"))
        pkg4 = r4["package_id"]

        # 5) 撤回预览
        preview = client.get(f"{args.app1}/api/packages/{pkg1}/withdraw/preview").json()
        affected = {a["package_id"] for a in preview["affected"]}
        results.append(check(
            "撤回闭包预览波及批次1/2/3，不含批次4",
            affected == {pkg1, pkg2, pkg3}, f"affected={sorted(affected)}"))
        print("    关联依据示例：")
        for a in preview["affected"]:
            print(f"      - {a['package_id']}: {'；'.join(a['basis'])}")

        # 6) 执行撤回（app1）
        wr = client.post(f"{args.app1}/api/packages/{pkg1}/withdraw",
                         json={"reason": "研究外误收，整包永久停供"})
        wr.raise_for_status()
        results.append(check(f"撤回成功，共 {len(wr.json()['affected'])} 个包停供",
                             len(wr.json()["affected"]) == 3))
        blocked = [client.get(f"{args.app2}/packages/{p}/download").status_code
                   for p in (pkg1, pkg2, pkg3)]
        results.append(check("三个包的新下载请求全部被拒（409）",
                             blocked == [409, 409, 409], str(blocked)))
        dl4 = client.get(f"{args.app1}/packages/{pkg4}/download")
        results.append(check("无关批次4 仍可从 app1 下载", dl4.status_code == 200))

        # 7) 撤回后再传：成员包含 / 序列引用
        r_member = wait_done(client, args.app2, upload(
            client, args.app2, pid,
            make_zip([("a-again.dcm", make_dicom(sop_uid=sop_a))]), "re-member.zip"))
        results.append(check(
            "app2 再传包含已撤回实例 A 的包 → 整包拒收",
            r_member["status"] == "rejected" and sop_a in (r_member["reason"] or ""),
            r_member.get("reason") or ""))
        r_ref = wait_done(client, args.app1, upload(
            client, args.app1, pid,
            make_zip([("ref-a.dcm", make_dicom(refs=[sop_a]))]), "re-ref.zip"))
        results.append(check(
            "app1 再传引用已撤回实例 A 的包 → 整包拒收",
            r_ref["status"] == "rejected" and sop_a in (r_ref["reason"] or ""),
            r_ref.get("reason") or ""))

        # 8) 无关包上传下载不受影响（跨实例）
        r_other = wait_done(client, args.app2, upload(
            client, args.app2, pid,
            make_zip([("f.dcm", make_dicom())]), "other.zip"))
        ok = r_other["status"] == "published" and client.get(
            f"{args.app1}/packages/{r_other['package_id']}/download").status_code == 200
        results.append(check("撤回后无关包跨实例上传下载正常", ok))

        # 9) 项目详情含撤回结论
        detail = client.get(f"{args.app2}/api/projects/{pid}").json()
        n_withdrawn = sum(1 for p in detail["packages"] if p["status"] == "withdrawn")
        results.append(check("项目详情记录 3 个已撤回包与撤回结论",
                             n_withdrawn == 3 and len(detail["withdrawals"]) == 3))

    print("\n结果：" + ("全部通过" if all(results) else "存在失败项"))
    print("浏览器演示：打开 http://localhost:8001 进入项目页，"
          "对任意已发布包点击“整包撤回”即可看到受影响包与关联依据。")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
