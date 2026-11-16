# DICOM 去标识化与撤回平台（纵向研究用）

影像数据工程师将同院 CT 检查交给纵向研究的网页工具：按研究项目上传含
1~20 份**单帧、未压缩、无像素烧录文字**的 DICOM ZIP，系统自动去标识化、
按项目稳定替换身份字段与 UID、递归同步序列内实例引用，并提供处理包下载。

研究 CT 包误收时，可在网页对已发布包发起**项目内撤回**：整包永久停供，
并按"共享 SOP 成员 / 引用已撤回实例"两条规则递归连带撤回，拒绝新下载、
拒收后续含撤回实例的上传，原件保留。

技术栈：FastAPI + pydicom + SQLite，支持双实例共享卷部署。

## 去标识化规则

| 项目 | 规则 |
| --- | --- |
| PatientName / PatientBirthDate | 递归删除（含序列条目内） |
| 私有标签 | 递归全部删除 |
| PatientID | 按项目稳定替换为伪名 `PT<12位哈希>` |
| Study/Series/SOP Instance UID | 按项目稳定替换为 `2.25.<UUID5>`（确定性，跨批次/跨实例一致） |
| 序列内实例引用 | 递归同步为映射后 UID；引用必须指向**本包内或本项目已发布实例**，否则整包拒收 |
| SOP Class UID / 传输语法 / 像素字节 | 保持不变 |
| 文件元信息 | MediaStorageSOPInstanceUID 同步为新 SOP UID |

输入校验（任一不满足即整包拒收，网页显示原因）：
ZIP 内 1~20 个文件、可解析为 DICOM、未压缩传输语法（Implicit/Explicit VR LE、
Explicit VR BE）、单帧（NumberOfFrames 缺省或 1）、`BurnedInAnnotation ≠ YES`、
具备 SOP Class/SOP/Study/Series UID、包内 SOPInstanceUID 不重复、上传 ≤512MiB。

## 撤回（永久停供）规则

在项目页对任意**已发布**包点击"整包撤回"，填写原因后执行：

1. **闭包计算（固定点）**：从入口包出发，递归应用两条规则直到范围不再扩大——
   - 规则一：与任一已撤回包**共享同一 SOP 成员实例**的包，整包连带撤回；
   - 规则二：任意层序列内 `ReferencedSOPInstanceUID` **引用了任一已撤回实例**
     的包，整包连带撤回（顶层与嵌套序列均覆盖，依据带元素路径）。
2. **单事务原子提交**：闭包内所有包在一个 `BEGIN IMMEDIATE` 事务内统一置为
   `withdrawn` 并写入 `withdrawals` 结论表；任何失败整次回滚，无部分生效。
3. **撤回成功后**：
   - 这些包的**新下载请求一律 409 拒绝**；处理包文件与原件均保留不删
     （已在途的下载可读完，停供靠目录状态而非删文件）；
   - 后续上传只要**成员包含或任意层序列引用已撤回实例**，发布事务内整包拒收，
     网页显示具体 UID 与触发位置；同实例同原文重新上传同样拒收（撤回优先于复用）；
   - 无关包上传下载完全不受影响。
4. **持久化**：撤回结论、成员/引用关系均在 SQLite 与共享卷中，重启后结论保留、
   不会复供。双实例交错发布与撤回由发布/撤回两个事务串行化保证不漏水。
5. 撤回网页先以只读预览列出**全部受影响包与关联依据**，确认后才提交；
   项目页另有撤回结论表（入口包/连带包、源头、原因、依据、时间）。

### 存量包关系补建（开放撤回的前置条件）

撤回闭包依赖每个包的成员关系与各层序列引用关系。新版本启动时自动在后台线程
解析原件 ZIP，为所有已发布存量包补齐 `package_members` / `package_references`：

- 补建期间**上传、下载照常可用**，撤回入口只读并显示横幅；
- 补建是幂等的，新发布的包在发布事务内自带关系，不会被漏记，也不进待补建列表；
- 全部存量包补齐（`app_meta.backfill_complete` 落标记）后才开放撤回；
  原件损坏的包会跳过并保留待补建状态，撤回继续关闭直至修复。

## 并发与一致性设计

- **目录库（SQLite，WAL）只存目录**：项目、包状态、已发布实例（原始 UID、
  原文 SHA-256、映射 UID）、包成员关系、序列引用关系、撤回结论。原件与处理包
  存共享卷 `originals/`、`packages/`。
- **同实例判重**：以原始文件字节 SHA-256 为准。双实例并发上传时，
  发布在 `BEGIN IMMEDIATE` 事务内完成"冲突检查 + 撤回闸门 + 实例登记 +
  成员/引用关系登记 + 包状态更新"：同实例同原文 → 复用；同实例异文 → 整包拒收；
  触及已撤回实例 → 整包拒收。
- **不留半包/未发布实例**：处理包先写 `staging/`，`os.replace` 原子改名后才
  提交发布事务；下载只认 `status='published'`。实例记录、关系与包状态同事务
  提交，不存在"可引用但未发布"的窗口。
- **启动恢复**：本实例遗留及超时停滞（10 分钟）的在途包标记失败；
  超过宽限期（60 秒）的无主 staging/孤儿包文件被清理；已撤回包的处理包文件
  保留；宽限期内的文件视为另一存活实例的在途工作，不受影响。
- **转换期间不阻塞**：处理在后台线程池执行，WAL 允许读写并发，
  其他上传与下载照常可用。

## 目录结构

```
app/
  deidentify.py   # 去标识化与引用同步核心
  references.py   # 递归提取各层序列 ReferencedSOPInstanceUID 关系
  store.py        # SQLite 目录库 + 共享卷布局 + 撤回闭包 + 发布闸门 + 启动恢复/补建
  pipeline.py     # 校验→引用提取→去标识化→staging→原子改名→事务发布
  main.py         # FastAPI 网页 + JSON API（含撤回页与后台补建调度）
examples/
  cross_batch_demo.py  # 跨批引用 + 双实例演示
  withdraw_demo.py     # 撤回闭包/闸门/双实例端到端演示
tests/            # 44 项自测（23 基线 + 21 撤回专项）
```

数据卷布局：`<DATA_DIR>/{originals,packages,staging}/` + `catalog.db`。

## 运行

### 本地单实例

```bash
pip install -r requirements.txt
DEID_DATA_DIR=./data uvicorn app.main:app --port 8000
# 打开 http://localhost:8000
```

### Docker Compose 双实例（共享卷）

```bash
docker compose up --build
# app1: http://localhost:8001   app2: http://localhost:8002
```

两个实例共享命名卷 `deid-data`（原件、处理包、SQLite 目录库）。
浏览器可在任一实例演示：建项目 → 上传 ZIP → 项目页点"整包撤回" →
查看受影响包与关联依据 → 确认撤回 → 尝试下载（被拒）→ 再传含撤回实例的包
（被拒收）。

### 端到端演示脚本

```bash
pip install -r requirements-dev.txt   # 需要 httpx
python3 examples/cross_batch_demo.py  # 去标识/判重/跨批引用（默认 8001/8002）
python3 examples/withdraw_demo.py     # 撤回闭包、下载/上传闸门、双实例
```

撤回演示覆盖：共享成员与引用两条连带规则、闭包预览依据、撤回后三包停供、
成员包含/序列引用两种再传拒收、无关包不受影响、撤回结论落盘。

### 自测

```bash
pip install -r requirements-dev.txt
python3 -m pytest        # 44 项：去标识化/引用/拒收/并发/恢复/API/撤回闭包/闸门/补建
```

## JSON API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/projects` | 创建项目 `{"name": ...}` |
| GET | `/api/projects/{id}` | 项目详情 + 包列表 + 撤回结论 + 补建状态 |
| POST | `/api/projects/{id}/packages` | 上传 ZIP（multipart `file`） |
| GET | `/api/packages/{id}` | 包状态与拒收/撤回原因 |
| GET | `/api/packages/{id}/withdraw/preview` | 撤回闭包预览（受影响包 + 关联依据） |
| POST | `/api/packages/{id}/withdraw` | 执行撤回 `{"reason": ...}`，返回受影响包 |
| GET | `/packages/{id}/download` | 下载处理包（仅已发布；已撤回 409） |
| GET | `/healthz` | 健康检查（含 `withdrawal_ready`） |

## 已知边界（未覆盖项）

- 仅删除需求指定的身份字段与私有标签；其他可能含身份信息的公开标签
  （如 InstitutionName、ReferringPhysicianName、自由文本）未做泛化处理。
- 像素级烧录文字依赖 `BurnedInAnnotation` 声明，不做 OCR 检测。
- 引用校验与撤回闭包覆盖 `ReferencedSOPInstanceUID`（各层序列）与
  `*InstanceUID` 类属性；FrameOfReferenceUID 等非实例引用保持原样、不参与闭包。
- 撤回为项目内作用域：连带关系只在同一项目内计算，不跨项目传播。
- 上传大小限制 512MiB，无认证鉴权（面向内网受信环境）。
