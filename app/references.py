"""递归提取 DICOM 各层序列内的 ReferencedSOPInstanceUID 关系。

撤回闭包与上传闸门都依赖"包引用了哪些 SOP 实例"这一关系：
- 顶层 ReferencedSOPInstanceUID 同样收集（path 标注位置）；
- 递归进入所有 SQ 序列（含嵌套序列），任何层级的
  ReferencedSOPInstanceUID 都不遗漏；
- 空值（type 2 空引用）跳过；同一 UID 多位置引用去重，保留首个路径。
"""
from __future__ import annotations


def extract_referenced_sop_uids(ds) -> list[tuple[str, str]]:
    """返回 [(referenced_sop_uid, 元素路径), ...]，按出现顺序去重。"""
    found: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _add(value, path: str) -> None:
        uid = str(value or "").strip()
        if uid and uid not in seen:
            seen.add(uid)
            found.append((uid, path))

    def _walk(elem_dataset, prefix: str) -> None:
        for elem in elem_dataset:
            keyword = elem.keyword or ""
            if elem.VR == "SQ":
                for i, item in enumerate(elem.value):
                    _walk(item, f"{prefix}{keyword}[{i + 1}].")
                continue
            if keyword == "ReferencedSOPInstanceUID":
                # 极少数情况下该属性可能是多值
                values = (
                    list(elem.value)
                    if hasattr(elem.value, "__iter__") and not isinstance(elem.value, str)
                    else [elem.value]
                )
                for v in values:
                    _add(v, f"{prefix}{keyword}")

    # 文件元信息内一般没有该属性，一并扫描以防实现差异
    _walk(ds.file_meta, "FileMeta.")
    _walk(ds, "")
    return found
