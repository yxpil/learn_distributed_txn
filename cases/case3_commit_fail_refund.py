# -*- coding: utf-8 -*-
"""案例3：正向提交途中失败 —— 用户已扣真实余额、商家提交被拒 -> 回滚并返还已扣余额。"""


def run(ctx):
    print("\n" + "=" * 64)
    print("案例3 提交中失败：用户已扣真实余额，商家 COMMIT 被下游拒绝 -> 返还余额 + 解冻库存")
    print("=" * 64)

    b0 = ctx.balance("u2")
    s0 = ctx.stock("m1", "s1")

    # 注入：下一次店家 COMMIT 抛业务性拒绝
    ctx.cluster.shop.admin("fail_commit", {"mode": "business"})
    res = ctx.gateway.order("u2", "m1", "s1", qty=1, amount=30)
    assert res.rolled_back, res

    print("  状态流转：")
    for line in ctx.transitions():
        print(line)

    b1 = ctx.balance("u2")
    s1 = ctx.stock("m1", "s1")
    print(f"  结果：余额={b1}（应返还回 {b0}），库存={s1}（应不变 {s0}）")
    assert b1 == b0, f"已扣余额应被返还: {b0} -> {b1}"
    assert s1 == s0, "库存不应变化（商家仅冻结未扣减）"
    assert ctx.executor.txn.get(res.txn_id)["state"] == "ROLLED_BACK"

    ctx.results.append(("案例3 提交中失败退款", True, "已扣余额返还，库存不变"))
    return True
