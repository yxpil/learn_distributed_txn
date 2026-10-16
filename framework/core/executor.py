# -*- coding: utf-8 -*-
"""
事务执行器：把"跨三个节点数据库的协调"全部收敛在业务层。
对外暴露虚拟原子操作 submit()；对内做 预冻结 -> 状态机 -> 分步提交/补偿。
硬性约束：
  - 任何资产变更都不绕过执行器直连节点（业务代码只能走 gateway.submit）
  - 每笔操作带全局事务ID + 幂等号，重试不重复扣减/返还
"""
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from ..errors import BusinessFailure, DbUnavailable
from ..proto import (
    INIT, FROZEN, ROLLBACK, ROLLED_BACK, SUCCESS, MANUAL_REVIEW, TERMINAL,
    PHASE_PENDING, PHASE_FROZEN, PHASE_COMMITTED, PHASE_COMPENSATED,
    OP_USER, OP_MERCHANT,
)
from .. import config
from .state_machine import validate_transition, is_terminal


@dataclass
class TxResult:
    txn_id: str
    state: str
    retrying: bool = False
    replayed: bool = False
    message: str = ""
    txn: Optional[dict] = None

    @property
    def success(self):
        return self.state == SUCCESS

    @property
    def rolled_back(self):
        return self.state == ROLLED_BACK

    @property
    def manual_review(self):
        return self.state == MANUAL_REVIEW


class TxnExecutor:
    def __init__(self, user, shop, txn, on_transition=None,
                 base_backoff=None, max_attempts=None, freeze_ttl=None):
        self.user = user
        self.shop = shop
        self.txn = txn
        self.on_transition = on_transition or (lambda tid, src, dst, rec: None)
        self.base_backoff = base_backoff if base_backoff is not None else config.BASE_BACKOFF
        self.max_attempts = max_attempts if max_attempts is not None else config.MAX_ATTEMPTS
        self.freeze_ttl = freeze_ttl if freeze_ttl is not None else config.FREEZE_TTL

    # ================= 对外：虚拟原子提交 =================
    def submit(self, ops, idempotency_key=None):
        """ops: [{"kind":"user","subject":uid,"amount":n}, {"kind":"merchant","merchant":m,"sku":s,"qty":n}]"""
        # 1) 幂等：相同幂等号直接返回既有结果，不重复执行
        if idempotency_key:
            for rec in self.txn.all().values():
                if rec.get("idempotency_key") == idempotency_key:
                    return TxResult(rec["txn_id"], rec["state"], replayed=True, txn=rec)

        # 2) 新建全局事务记录，状态=INIT
        txn_id = uuid.uuid4().hex[:16]
        now = time.time()
        record = {
            "txn_id": txn_id,
            "idempotency_key": idempotency_key,
            "state": INIT,
            "created_at": now,
            "updated_at": now,
            "ops": [dict(op, phase=PHASE_PENDING, txn_id=txn_id) for op in ops],
            "attempts": 0,
            "next_retry_at": None,
            "last_error": None,
        }
        self.txn.create(record)
        self._fire(txn_id, None, INIT, record)
        return self._freeze_phase(txn_id)

    # ================= 冻结阶段 =================
    def _freeze_phase(self, txn_id):
        rec = self._get(txn_id)
        try:
            for op in rec["ops"]:
                if op["phase"] == PHASE_FROZEN:
                    continue  # 重试幂等：已冻结的跳过
                self._do_freeze(op, txn_id)
                op["phase"] = PHASE_FROZEN
                self._persist_ops(txn_id, rec)
            # 全部冻结成功 -> FROZEN
            self._transition(txn_id, FROZEN)
            return self._commit_phase(txn_id)
        except DbUnavailable as e:
            return self._schedule_retry(txn_id, e)
        except BusinessFailure as e:
            return self._rollback(txn_id, reason=str(e))

    def _do_freeze(self, op, txn_id):
        expire_at = time.time() + self.freeze_ttl
        if op["kind"] == OP_USER:
            self.user.freeze(op["subject"], txn_id, op["amount"], expire_at)
        elif op["kind"] == OP_MERCHANT:
            self.shop.freeze(op["merchant"], op["sku"], txn_id, op["qty"], expire_at)
        else:
            raise BusinessFailure(f"未知操作类型: {op.get('kind')}")

    # ================= 正向提交阶段 =================
    def _commit_phase(self, txn_id):
        rec = self._get(txn_id)
        try:
            for op in rec["ops"]:
                if op["phase"] == PHASE_COMMITTED:
                    continue  # 重试幂等
                self._do_commit(op, txn_id)
                op["phase"] = PHASE_COMMITTED
                self._persist_ops(txn_id, rec)
            self._transition(txn_id, SUCCESS)
            return TxResult(txn_id, SUCCESS, txn=self._get(txn_id))
        except DbUnavailable as e:
            return self._schedule_retry(txn_id, e)
        except BusinessFailure as e:
            return self._rollback(txn_id, reason=str(e))

    def _do_commit(self, op, txn_id):
        if op["kind"] == OP_USER:
            self.user.commit(op["subject"], txn_id, op["amount"])
        elif op["kind"] == OP_MERCHANT:
            self.shop.commit(op["merchant"], op["sku"], txn_id, op["qty"])
        else:
            raise BusinessFailure(f"未知操作类型: {op.get('kind')}")

    # ================= 补偿回滚 =================
    def _rollback(self, txn_id, reason=""):
        rec = self._get(txn_id)
        if is_terminal(rec["state"]):
            return TxResult(txn_id, rec["state"], txn=rec)
        self._transition(txn_id, ROLLBACK, extra={"last_error": reason or rec.get("last_error")})
        try:
            for op in rec["ops"]:
                if op["phase"] == PHASE_COMPENSATED:
                    continue
                if op["phase"] == PHASE_COMMITTED:
                    self._do_refund(op, txn_id)     # 已扣真实值 -> 返还
                elif op["phase"] == PHASE_FROZEN:
                    self._do_unfreeze(op, txn_id)   # 只冻结 -> 解冻
                op["phase"] = PHASE_COMPENSATED
                self._persist_ops(txn_id, rec)
            self._transition(txn_id, ROLLED_BACK)
            return TxResult(txn_id, ROLLED_BACK, message=reason, txn=self._get(txn_id))
        except DbUnavailable as e:
            return self._schedule_retry(txn_id, e)  # 保持 ROLLBACK，等死信/节点自检续做
        except BusinessFailure as e:
            self._transition(txn_id, MANUAL_REVIEW, extra={"last_error": str(e)})
            config.alert("manual_review", f"[{txn_id}] 补偿遇到不可恢复业务错误: {e}", txn_id)
            return TxResult(txn_id, MANUAL_REVIEW, message=str(e), txn=self._get(txn_id))

    def _do_refund(self, op, txn_id):
        if op["kind"] == OP_USER:
            self.user.refund(op["subject"], txn_id, op["amount"])
        elif op["kind"] == OP_MERCHANT:
            self.shop.add_back(op["merchant"], op["sku"], txn_id, op["qty"])
        else:
            raise BusinessFailure(f"未知操作类型: {op.get('kind')}")

    def _do_unfreeze(self, op, txn_id):
        if op["kind"] == OP_USER:
            self.user.unfreeze(op["subject"], txn_id)
        elif op["kind"] == OP_MERCHANT:
            self.shop.unfreeze(op["merchant"], op["sku"], txn_id)
        else:
            raise BusinessFailure(f"未知操作类型: {op.get('kind')}")

    # ================= 重试调度（死信/轮询器复用） =================
    def drive(self, txn_id):
        """把一笔非终态事务向终态推进（轮询器/死信消费调用）。"""
        rec = self._get(txn_id)
        if rec is None or is_terminal(rec["state"]):
            return TxResult(txn_id, rec["state"] if rec else "GONE", txn=rec)
        state = rec["state"]
        if state == INIT:
            return self._freeze_phase(txn_id)
        if state == FROZEN:
            return self._commit_phase(txn_id)
        if state == ROLLBACK:
            return self._rollback(txn_id, reason=rec.get("last_error") or "")
        return TxResult(txn_id, state, txn=rec)

    def retry_or_review(self, txn_id, err):
        """死信消费的退避入口：递增尝试次数，未达上限则安排下次重试，否则进人工复核。"""
        return self._schedule_retry(txn_id, err)

    def force_rollback(self, txn_id, reason="business cancel"):
        """业务方主动取消：把事务推向回滚（即便节点暂时不可达，由自检/死信续做）。"""
        return self._rollback(txn_id, reason=reason)

    # ================= 内部工具 =================
    def _get(self, txn_id):
        rec = self.txn.get(txn_id)
        if rec is None:
            raise RuntimeError(f"事务记录丢失: {txn_id}")
        return rec

    def _persist_ops(self, txn_id, rec):
        self.txn.update(txn_id, ops=rec["ops"], updated_at=time.time())

    def _transition(self, txn_id, dst, extra=None):
        rec = self._get(txn_id)
        src = rec["state"]
        validate_transition(src, dst)
        fields = {"state": dst, "updated_at": time.time()}
        if extra:
            fields.update(extra)
        self.txn.update(txn_id, **fields)
        self._fire(txn_id, src, dst, {**rec, **fields})

    def _fire(self, txn_id, src, dst, rec):
        self.on_transition(txn_id, src, dst, rec)

    def _schedule_retry(self, txn_id, err):
        rec = self._get(txn_id)
        rec["attempts"] = rec.get("attempts", 0) + 1
        rec["last_error"] = str(err)
        if rec["attempts"] >= self.max_attempts:
            self.txn.update(txn_id, attempts=rec["attempts"],
                            last_error=str(err), state=MANUAL_REVIEW, updated_at=time.time())
            self._fire(txn_id, rec["state"], MANUAL_REVIEW, self._get(txn_id))
            config.alert("manual_review",
                         f"[{txn_id}] 重试耗尽({rec['attempts']})，进入人工复核: {err}", txn_id)
            return TxResult(txn_id, MANUAL_REVIEW, message=str(err), txn=self._get(txn_id))
        backoff = self.base_backoff * (2 ** (rec["attempts"] - 1))
        rec["next_retry_at"] = time.time() + backoff
        self.txn.update(txn_id, attempts=rec["attempts"], last_error=str(err),
                        next_retry_at=rec["next_retry_at"], updated_at=time.time())
        return TxResult(txn_id, rec["state"], retrying=True,
                        message=f"第{rec['attempts']}次尝试失败: {err}", txn=rec)
