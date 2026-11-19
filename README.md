# 源到达SLA

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 源到达SLA |
| 目录 | `06-lagboard` |
| 领域建议 | 数据采集、爬虫与 RPA |
| 接口 | http://127.0.0.1:10838 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

源时钟 vs 摄入时钟、分级处罚、降级可读不可写、补齐解冻

## 避开

别做成纯监控大盘；须有写路径与状态机

## 中断切断（冻结级联）口径

- 本仓只走**甲路径**：父子源冻结捆成同一锁事务，任一节点遇挫则父子同撤（`rollback`），正式侧保持旧值，禁止半树冻结；不混用乙路径（无影子表、无分段落库）。
- 父子拓扑唯一来源是 `sources.parent_id`（递归 CTE 读子树），编排画布与源目录牌同树，禁止另起第二套连线或第二台中断台。
- `POST /api/cutoff/{源}` 显式切断；到达登记判违约时自动走同一甲事务。切断后该源及子树只读，再写到达返回 409。
- 封档写入 `freeze_cutoffs`（按根源+日期唯一，当天不可改口，次日方可 `/api/catchup` 解冻）；`freeze_locks` 是双席抢冻的中断锁，后交只在 `freeze_events` 记一条「争用」；`freeze_events` 为只追加专册，类别含 封档/争用/传递失败/偏卡。
- 所有表均在 SQLite `data/app.db`，不挂外置 Postgres。

## 互斥切口候选

- 超SLA冻结 vs 降级只读
- 补齐整源解冻 vs 按业务日窗口解冻
- 只认源时钟 vs 双时钟取严

## 启动（一键）

`powershell
cd projects/06-lagboard
docker compose up -d --build
`

浏览器：http://127.0.0.1:10838/  
健康检查：GET http://127.0.0.1:10838/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10603
python -m pytest -q
`
