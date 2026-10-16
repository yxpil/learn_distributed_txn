# -*- coding: utf-8 -*-
"""对外 API 网关：业务调用入口，虚拟原子事务接口。
业务代码不直连任何数据库节点，全部走这里；资产变更强制经过网关。"""


class Gateway:
    def __init__(self, executor):
        self.executor = executor

    # ---------- 业务入口（虚拟原子操作） ----------
    def order(self, user_id, merchant_id, sku, qty, amount, idempotency_key=None):
        """示例业务：用户扣余额 + 商家扣库存。对业务方表现为"一笔原子事务"。"""
        ops = [
            {"kind": "user", "subject": user_id, "amount": amount},
            {"kind": "merchant", "merchant": merchant_id, "sku": sku, "qty": qty},
        ]
        return self.executor.submit(ops, idempotency_key=idempotency_key)

    def cancel(self, txn_id, reason="business cancel"):
        return self.executor.force_rollback(txn_id, reason=reason)

    def status(self, txn_id):
        return self.executor.txn.get(txn_id)

    # ---------- 只读查询（也不直连业务侧以外的写） ----------
    def user_balance(self, user_client, user_id):
        u = user_client.get(user_id) or {}
        return u.get("balance")

    def shop_stock(self, shop_client, merchant_id, sku):
        sk = shop_client._c.call("shop_get_sku", {"merchant_id": merchant_id, "sku": sku}) or {}
        return sk.get("stock")
