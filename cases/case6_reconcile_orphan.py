# -*- coding: utf-8 -*-
"""案例6：兜底离线对账 —— 检出孤儿冻结（有冻结标记无全局单据）自动解冻告警；
全局事务状态库主数据丢失时提示可从快照备份恢复。"""
import time


def run(ctx):
    print("\n" + "=" * 64)
    print("案例6 离线对账：孤儿冻结自动解冻 + 全局状态库丢失可备份恢复")
    print("=" * 64)

    # 1) 注入一笔"孤儿冻结"：本地有冻结标记，但全局事务状态库没有对应单据
    ctx.user.freeze("u1", "orphan_txn_0001", 20, time.time() + 3600)
    print("  注入孤儿冻结：user/u1 存在冻结标记 orphan_txn_0001，但全局无此事务单据")

    # 全局状态库定期快照备份
    backup = ctx.txn.snapshot_backup()
    print(f"  已做全局事务状态库快照备份：{backup['backup']}")

    report = ctx.reconciler.run()
    orphan = [o for o in report["orphan_freezes"] if o["txn_id"] == "orphan_txn_0001"]
    assert orphan, "应检出孤儿冻结"
    assert orphan[0]["node"] == "user"
    print("  对账检出孤儿冻结 -> 自动解冻 + 告警。")
    for al in [a for a in ctx.alerts() if a["kind"] == "orphan_unfreeze"]:
        print(f"    告警: {al['message']}")

    frozen_after = (ctx.user.get("u1") or {}).get("frozen", {})
    assert "orphan_txn_0001" not in frozen_after, "孤儿冻结应已被解冻"
    assert ctx.balance("u1") == 70, "解冻孤儿不应改变真实余额"
    print("  孤儿冻结已清理，u1 余额不变（=70）。")

    # 2) 模拟全局事务状态库主数据彻底损毁（无有效备份的极端场景）
    ctx.txn.wipe()
    report2 = ctx.reconciler.run()
    assert report2["missing_records"], "全局状态库为空应提示备份恢复"
    print("  全局事务状态库主数据被清空（模拟彻底损毁）-> 对账提示可用备份恢复。")

    restore = ctx.txn.restore_backup(newest=True)
    print(f"  从快照恢复：{restore['restored']}，恢复事务单据 {restore['txn_count']} 条")

    ctx.results.append(("案例6 离线对账", True, "孤儿冻结自动解冻+告警，全局库可备份恢复"))
    return True
