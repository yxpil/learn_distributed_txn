# -*- coding: utf-8 -*-
"""案例1：正常下单 —— INIT -> FROZEN -> SUCCESS，且幂等号重放不重复扣减。"""


def run(ctx):
    c = ctx
    print("\n" + "=" * 64)
    print("案例1 正常下单：用户扣余额 + 商家扣库存，对外表现为原子事务")
    print("=" * 64)

    b0 = ctx.balance("u1")
    s0 = ctx.stock("m1", "s1")
    print(f"  初始：用户 u1 余额={b0}，商家 m1/s1 库存={s0}")

    res = ctx.gateway.order("u1", "m1", "s1", qty=1, amount=30, idempotency_key="case1-order")
    assert res.success, res

    print("  状态流转：")
    for line in ctx.transitions():
        print(line)

    b1 = ctx.balance("u1")
    s1 = ctx.stock("m1", "s1")
    print(f"  结果：余额={b1}（应 {b0-30}），库存={s1}（应 {s0-1}）")
    assert b1 == b0 - 30, (b0, b1)
    assert s1 == s0 - 1, (s0, s1)

    # 幂等重放：同一幂等号再次提交，直接返回既有结果，不重复扣减
    res2 = ctx.gateway.order("u1", "m1", "s1", qty=1, amount=30, idempotency_key="case1-order")
    assert res2.replayed and res2.state == "SUCCESS"
    assert ctx.balance("u1") == b1, "幂等重放不应再次扣减余额"
    assert ctx.stock("m1", "s1") == s1, "幂等重放不应再次扣减库存"
    print("  幂等验证通过：同一幂等号重放直接返回既有结果，无重复扣减。")

    ctx.results.append(("案例1 正常下单", True, "INIT->FROZEN->SUCCESS，幂等重放通过"))
    return True
