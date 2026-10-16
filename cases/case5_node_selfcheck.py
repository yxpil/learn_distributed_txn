# -*- coding: utf-8 -*-
"""案例5【第二层自愈】：店家节点宕机，业务层无法下发回滚 -> 节点重启后自治自检回滚。
流程：冻结+提交中店家瞬时故障 -> 事务 FROZEN；杀掉店家进程并强制回滚（补偿够不到店家）
-> 重启店家节点，启动自检查到 ROLLBACK 事务 + 本地冻结标记 -> 本地解冻 -> 全局 ROLLED_BACK。
"""
import time


def run(ctx):
    print("\n" + "=" * 64)
    print("案例5 节点自治回滚：店家宕机期间无法下发回滚，重启后节点自检自行补偿")
    print("=" * 64)

    b0 = ctx.balance("u2")
    s0 = ctx.stock("m1", "s1")

    # 1) 注入：店家 COMMIT 瞬时故障 -> 用户已扣(COMMITTED)，店家仅冻结 -> 事务滞留 FROZEN
    ctx.cluster.shop.admin("fail_commit", {"mode": "unavailable"})
    res = ctx.gateway.order("u2", "m1", "s1", qty=1, amount=30)
    assert res.retrying and res.state == "FROZEN", res
    tid = res.txn_id
    print(f"  1) 冻结双方成功 -> FROZEN；店家 COMMIT 瞬时故障，事务滞留 FROZEN（attempts=1）")

    # 2) 杀掉店家进程（模拟宕机），业务层强制回滚：用户可返还，店家补偿够不到
    ctx.cluster.stop("shop")
    r = ctx.gateway.cancel(tid, reason="商家宕机，业务取消")
    assert r.state == "ROLLBACK", r
    print(f"  2) 店家进程已宕机；强制回滚：用户已扣余额已返还，店家解冻补偿因节点不可达而挂起，"
          f"事务滞留 ROLLBACK（attempts={ctx.executor.txn.get(tid)['attempts']}）")

    # 3) 重启店家节点 -> 启动自检插件自治补偿
    print("  3) 重启店家节点，启动自检插件查询未完成事务并核对本地冻结...")
    ctx.cluster.restart("shop")
    time.sleep(1.2)  # 等待自检线程跑完

    rec = ctx.executor.txn.get(tid)
    print("  状态流转：")
    for line in ctx.transitions():
        print(line)
    print(f"  终态：{rec['state']}")

    b1 = ctx.balance("u2")
    s1 = ctx.stock("m1", "s1")
    print(f"  结果：余额={b1}（应返还回 {b0}），库存={s1}（应不变 {s0}）")
    assert rec["state"] == "ROLLED_BACK", rec
    assert b1 == b0, f"用户余额应返还: {b0} -> {b1}"
    assert s1 == s0, "店家库存应通过自检解冻恢复原值"

    ctx.results.append(("案例5 节点自治回滚", True, "店家重启后自检补偿，事务 ROLLED_BACK"))
    return True
