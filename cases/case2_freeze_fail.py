# -*- coding: utf-8 -*-
"""案例2：冻结阶段业务失败 —— 用户余额不足，商家已冻结 -> 立即回滚解冻，库存不变。"""


def run(ctx):
    print("\n" + "=" * 64)
    print("案例2 冻结失败：用户余额不足，冻结中途失败 -> ROLLBACK -> ROLLED_BACK")
    print("=" * 64)

    b0 = ctx.balance("u_low")
    s0 = ctx.stock("m1", "s1")
    print(f"  初始：用户 u_low 余额={b0}，商家 m1/s1 库存={s0}")

    res = ctx.gateway.order("u_low", "m1", "s1", qty=1, amount=999)  # 余额不足
    assert res.rolled_back, res

    print("  状态流转：")
    for line in ctx.transitions():
        print(line)

    b1 = ctx.balance("u_low")
    s1 = ctx.stock("m1", "s1")
    print(f"  结果：余额={b1}，库存={s1}（商家冻结已被解冻，库存不变）")
    assert b1 == b0, "余额不应变化"
    assert s1 == s0, "商家库存不应变化（冻结即解冻）"
    assert ctx.executor.txn.get(res.txn_id)["state"] == "ROLLED_BACK"

    ctx.results.append(("案例2 冻结失败回滚", True, "商家已冻结->解冻，库存不变"))
    return True
