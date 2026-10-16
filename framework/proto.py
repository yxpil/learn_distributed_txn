# -*- coding: utf-8 -*-
"""
节点间 / 框架与节点间的通信协议常量。
数据在节点内以 JSON 落盘；节点间、框架与节点间以 JSON over HTTP 交互。
"""

# ---- 事务状态（与设计文档一致） ----
INIT = "INIT"
FROZEN = "FROZEN"
ROLLBACK = "ROLLBACK"
ROLLED_BACK = "ROLLED_BACK"
SUCCESS = "SUCCESS"
MANUAL_REVIEW = "MANUAL_REVIEW"

TERMINAL = frozenset({ROLLED_BACK, SUCCESS, MANUAL_REVIEW})
ALL_STATES = (INIT, FROZEN, ROLLBACK, ROLLED_BACK, SUCCESS, MANUAL_REVIEW)

# ---- 操作种类（一笔事务里的原子步骤） ----
OP_USER = "user"        # 用户资产：扣余额
OP_MERCHANT = "merchant"  # 店家库存：扣库存

# ---- 步骤相位（执行器为每步记录的真实状态，用于精确补偿） ----
PHASE_PENDING = "PENDING"       # 尚未冻结
PHASE_FROZEN = "FROZEN"         # 已冻结（未扣真实值）
PHASE_COMMITTED = "COMMITTED"   # 已扣真实值
PHASE_COMPENSATED = "COMPENSATED"  # 已补偿（解冻/返还）

# ---- 节点 HTTP 路由 ----
ROUTE_OP = "/op"            # 业务操作
ROUTE_HEALTH = "/health"    # 健康检查
ROUTE_ADMIN = "/admin"      # 运维：宕机模拟 / 种子数据 / 重启自检

# ---- 用户节点命令 ----
CMD_SEED_USER = "seed_user"
CMD_USER_FREEZE = "user_freeze"
CMD_USER_COMMIT = "user_commit"
CMD_USER_REFUND = "user_refund"
CMD_USER_UNFREEZE = "user_unfreeze"
CMD_USER_GET = "user_get"
CMD_USER_MARKERS = "user_markers"     # 全量冻结标记（对账/自检用）
CMD_USER_SNAPSHOT = "user_snapshot"   # 全量数据（对账用）

# ---- 店家节点命令 ----
CMD_SEED_SHOP = "seed_shop"
CMD_SHOP_FREEZE = "shop_freeze"
CMD_SHOP_COMMIT = "shop_commit"
CMD_SHOP_ADD_BACK = "shop_add_back"
CMD_SHOP_UNFREEZE = "shop_unfreeze"
CMD_SHOP_MARKERS = "shop_markers"
CMD_SHOP_SNAPSHOT = "shop_snapshot"
CMD_SHOP_GET_SKU = "shop_get_sku"

# ---- 事务节点命令 ----
CMD_TXN_CREATE = "txn_create"
CMD_TXN_UPDATE = "txn_update"
CMD_TXN_GET = "txn_get"
CMD_TXN_ALL = "txn_all"
CMD_TXN_NON_TERMINAL = "txn_non_terminal"
CMD_TXN_SNAPSHOT_BACKUP = "txn_snapshot_backup"
CMD_TXN_WIPE = "txn_wipe"           # 仅演示：模拟全局状态库数据丢失
CMD_TXN_RESTORE_BACKUP = "txn_restore_backup"  # 仅演示：从备份恢复

# ---- 运维命令 ----
ADMIN_OUTAGE = "outage"      # 模拟分片宕机/恢复
ADMIN_RESTART_SELFCHECK = "restart_selfcheck"  # 触发一次"节点重启后自治自检"
ADMIN_FAIL_COMMIT = "fail_commit"  # 注入：下一次店家 COMMIT 抛异常（business/unavailable）
