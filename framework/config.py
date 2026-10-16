# -*- coding: utf-8 -*-
"""全局配置：节点地址、重试策略、冻结超时、告警收集。"""

# ---- 三个 node 服务器的监听地址（进程级模拟） ----
USER_NODE_PORT = int(__import__("os").environ.get("USER_NODE_PORT", "6001"))
SHOP_NODE_PORT = int(__import__("os").environ.get("SHOP_NODE_PORT", "6002"))
TXN_NODE_PORT  = int(__import__("os").environ.get("TXN_NODE_PORT",  "6003"))
USER_NODE_URL = f"http://127.0.0.1:{USER_NODE_PORT}"
SHOP_NODE_URL = f"http://127.0.0.1:{SHOP_NODE_PORT}"
TXN_NODE_URL  = f"http://127.0.0.1:{TXN_NODE_PORT}"

# 节点 HTTP 请求超时（秒）
NODE_HTTP_TIMEOUT = 3.0

# ---- 事务重试策略（死信处理用） ----
BASE_BACKOFF = 0.3          # 指数退避基数（秒）：1 次 0.3，2 次 0.6，3 次 1.2 ...
MAX_ATTEMPTS = 3            # 最大尝试次数，超过即进入 MANUAL_REVIEW
FREEZE_TTL   = 30.0         # 冻结标记超时（秒），供节点自治回滚判定"该事务已死"

# ---- 分片 / 副本参数 ----
USER_SHARDS = 4             # 用户资产库：用户ID水平分片数
SHOP_SHARDS = 2             # 店家库存库：按商家分库数
TXN_REPLICAS = 2            # 全局事务状态库：副本数

# ---- 告警收集（任何模块都能往里丢告警） ----
ALERTS = []                 # list[dict(kind, message, txn_id, ts)]

def alert(kind, message, txn_id=None):
    import time
    rec = {"kind": kind, "message": message, "txn_id": txn_id, "ts": time.time()}
    ALERTS.append(rec)
    return rec
