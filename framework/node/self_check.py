# -*- coding: utf-8 -*-
"""
【第二层自愈：节点启动自检 + 自治回滚插件】
嵌入每个用户/店家节点。当节点从宕机/隔离恢复上线时：
1. 主动请求全局事务状态中心，拉取所有"关联本库、未完成"的事务；
2. 逐条核对本库本地资产的冻结标记；
3. 判定需要回滚（状态=ROLLBACK，或冻结已超时判定事务死亡）→ 在本库本地事务内
   自动执行解冻/返还补偿，完成后回写全局事务状态。
解决"故障期间外部业务连不上、无法推送回退指令"的场景：补偿延后到节点恢复后自治完成。
"""
import time

from ..errors import BusinessFailure, DbUnavailable
from ..proto import (
    ROLLBACK, ROLLED_BACK, INIT, FROZEN, TERMINAL,
    PHASE_FROZEN, PHASE_COMMITTED, PHASE_COMPENSATED, OP_USER, OP_MERCHANT,
)
from ..config import alert, FREEZE_TTL


class SelfCheckPlugin:
    def __init__(self, logic, txn_client, coordinator_unreachable_ok=True):
        """
        logic: 本节点的 NodeLogicBase（user/shop）
        txn_client: 全局事务状态中心客户端
        """
        self.logic = logic
        self.txn = txn_client
        self.role = logic.ROLE
        self.coordinator_unreachable_ok = coordinator_unreachable_ok

    # ---------- 找出"关联本节点"的操作 ----------
    def _my_ops(self, record):
        out = []
        for op in record.get("ops", []):
            if op["kind"] == OP_USER and self.role == "user_db":
                out.append(op)
            elif op["kind"] == OP_MERCHANT and self.role == "shop_db":
                out.append(op)
        return out

    def _op_is_mine(self, marker, op):
        if op["kind"] == OP_USER:
            return marker.get("user_id") == op.get("subject")
        if op["kind"] == OP_MERCHANT:
            return (marker.get("merchant_id") == op.get("merchant")
                    and marker.get("sku") == op.get("sku"))
        return False

    def _freeze_expired(self, record, now=None):
        now = now or time.time()
        expires = [op.get("expire_at") for op in record.get("ops", [])]
        expires = [e for e in expires if e is not None]
        return bool(expires) and all(e < now for e in expires)

    # ---------- 单步本地补偿 ----------
    def _compensate_one(self, marker, op):
        """对一笔属于本节点的操作做本地补偿，返回是否已解决（phase 置 COMPENSATED）。"""
        if op.get("phase") == PHASE_COMPENSATED:
            return True
        if self.role == "user_db":
            uid = op.get("subject")
            if op.get("phase") == PHASE_COMMITTED:
                self.logic.refund(uid, op["txn_id"], op["amount"])
            elif op.get("phase") == PHASE_FROZEN:
                self.logic.unfreeze(uid, op["txn_id"])
            else:  # PENDING：本地无标记可解
                return False
        elif self.role == "shop_db":
            mid, sku = op.get("merchant"), op.get("sku")
            if op.get("phase") == PHASE_COMMITTED:
                self.logic.add_back(mid, sku, op["txn_id"], op["qty"])
            elif op.get("phase") == PHASE_FROZEN:
                self.logic.unfreeze(mid, sku, op["txn_id"])
            else:
                return False
        op["phase"] = PHASE_COMPENSATED
        return True

    # ---------- 主流程：节点恢复上线后执行一次 ----------
    def run_once(self):
        if self.txn is None:
            return {"role": self.role, "compensated": [], "note": "无事务中心可查"}
        try:
            unfinished = self.txn.non_terminal()
        except DbUnavailable as e:
            if self.coordinator_unreachable_ok:
                # 事务中心也不可用：把待补偿记录暂存本地，稍后重试（演示从简）
                return {"role": self.role, "compensated": [],
                        "note": f"事务中心暂不可达，稍后重试: {e}"}
            raise

        my_markers = self.logic.all_frozen_markers()
        done = []

        # 1) 先按"我本地的冻结标记"逐笔核对
        for txn_id, markers in my_markers.items():
            record = self.txn.get(txn_id)
            if record is None:
                # 孤儿冻结：本地有标记，全局却查不到记录 -> 本地解冻 + 告警
                for m in markers:
                    self._drop_marker(m, txn_id)
                alert("orphan_unfreeze",
                      f"[{self.role}] 自检发现孤儿冻结 {txn_id}，已在本地解冻", txn_id)
                done.append(("orphan", txn_id))
                continue

            state = record.get("state")
            my_ops = self._my_ops(record)

            # 2) 判定是否需要本节点自治回滚
            need_rollback = (state == ROLLBACK) or self._freeze_expired(record)
            if state in TERMINAL:
                if state in (ROLLED_BACK, SUCCESS) and my_markers.get(txn_id):
                    for m in markers:
                        self._drop_marker(m, txn_id)
                    alert("leftover_marker",
                          f"[{self.role}] 终态 {state} 仍有冻结标记残留，已清理", txn_id)
                continue

            if need_rollback:
                self._autonomous_rollback(record, txn_id, my_ops)
                done.append(("rollback", txn_id))

        return {"role": self.role, "compensated": done}

    def _drop_marker(self, marker, txn_id):
        try:
            if self.role == "user_db":
                self.logic.unfreeze(marker["user_id"], txn_id)
            elif self.role == "shop_db":
                self.logic.unfreeze(marker["merchant_id"], marker["sku"], txn_id)
        except (BusinessFailure, DbUnavailable) as e:
            alert("selfcheck_error", f"[{self.role}] 解冻 {txn_id} 失败: {e}", txn_id)

    def _autonomous_rollback(self, record, txn_id, my_ops):
        """判定需要回滚 -> 本地补偿本节点负责的操作 -> 回写全局状态。"""
        changed = False
        for op in my_ops:
            marker = next((m for m in self.logic.all_frozen_markers().get(txn_id, [])
                           if self._op_is_mine(m, op)), None)
            if self._compensate_one(marker, op):
                changed = True

        # 回写全局：更新本节点负责的操作相位
        if changed:
            try:
                self.txn.update(txn_id, ops=record.get("ops", []))
            except DbUnavailable as e:
                alert("selfcheck_sync_pending",
                      f"[{self.role}] 回写 {txn_id} 失败，等待事务中心恢复: {e}", txn_id)
                return

        # 若全部操作都已补偿 -> 事务可终态 ROLLED_BACK
        if all(op.get("phase") == PHASE_COMPENSATED for op in record.get("ops", [])):
            try:
                self.txn.update(txn_id, state=ROLLED_BACK)
            except DbUnavailable:
                pass
        else:
            # 仍有其他节点的操作未补偿：至少把本节点判定改为 ROLLBACK，交由其余节点/轮询器完成
            try:
                self.txn.update(txn_id, state=ROLLBACK)
            except DbUnavailable:
                pass
