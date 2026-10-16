# -*- coding: utf-8 -*-
"""
第一层自愈（下半）：死信处理模块。
消费逻辑（与设计文档一致）：
  1. 重连下游数据库（探活涉及的节点/分片）；
  2. 先核对数据库真实资产状态与事务记录是否一致；
  3. 决定下一步动作（继续正向 / 启动补偿），由执行器推进；
  4. 使用指数退避重试（base * 2^n）；
  5. 达到最大次数不再无限重试 -> 标记 MANUAL_REVIEW，触发告警。
"""
import time

from ..errors import DbUnavailable
from ..proto import PHASE_FROZEN, PHASE_COMMITTED, OP_USER, OP_MERCHANT
from .. import config
from .poller import Poller  # noqa: F401  便于外部 import


class DeadLetter:
    def __init__(self, executor, user, shop, ping_timeout=2.0):
        self.executor = executor
        self.user = user
        self.shop = shop
        self.ping_timeout = ping_timeout

    def _involved_reachable(self, rec):
        """重连：探活这笔事务涉及的节点/分片是否可达。"""
        for op in rec.get("ops", []):
            if op["kind"] == OP_USER:
                h = self.user._c.health()
                if not h or h.get("data", {}).get("status") != "up":
                    return False
            elif op["kind"] == OP_MERCHANT:
                h = self.shop._c.health()
                if not h or h.get("data", {}).get("status") != "up":
                    return False
        return True

    def _verify_consistency(self, rec):
        """核对真实资产状态：每个已冻结/已扣减的操作，节点上应存在/不存在对应标记。"""
        notes = []
        for op in rec.get("ops", []):
            try:
                if op["kind"] == OP_USER:
                    u = self.user.get(op["subject"]) or {}
                    has = (op["txn_id"] in (u.get("frozen") or {}))
                else:
                    sk = self.shop._c.call(
                        "shop_get_sku", {"merchant_id": op["merchant"], "sku": op["sku"]}) or {}
                    has = (op["txn_id"] in (sk.get("frozen") or {}))
            except Exception:
                continue
            if op["phase"] == PHASE_FROZEN and not has:
                notes.append(f"{op['kind']}:{op.get('subject', op.get('sku'))} 无冻结标记但状态为已冻结")
            if op["phase"] == PHASE_COMMITTED and has:
                notes.append(f"{op['kind']}:{op.get('subject', op.get('sku'))} 已扣减但仍有冻结标记")
        return notes

    def process(self, txn_id):
        rec = self.executor.txn.get(txn_id)
        if rec is None:
            return {"txn_id": txn_id, "action": "gone"}
        from ..proto import TERMINAL
        if rec["state"] in TERMINAL:
            return {"txn_id": txn_id, "action": "terminal"}

        # 1) 重连核对
        if not self._involved_reachable(rec):
            result = self.executor.retry_or_review(
                txn_id, DbUnavailable("死信消费：下游节点暂不可达，退避重试"))
            return {"txn_id": txn_id, "action": "backoff", "result": result}

        # 2) 真实资产状态核对（发现漂移则告警但不阻断推进）
        for note in self._verify_consistency(rec):
            config.alert("drift_detected", f"[{txn_id}] {note}", txn_id)

        # 3) 继续推进（继续正向 或 启动补偿，由状态机决定）
        result = self.executor.drive(txn_id)
        return {"txn_id": txn_id, "action": "drive", "result": result}
