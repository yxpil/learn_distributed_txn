# -*- coding: utf-8 -*-
"""案例7：死信重试耗尽 -> MANUAL_REVIEW 人工复核（不再无限重试）。"""
import time


def run(ctx):
    print("\n" + "=" * 64)
    print("案例7 重试耗尽：店家持续不可达，指数退避达到上限 -> MANUAL_REVIEW 触发告警")
    print("=" * 64)

    shard = ctx.shard("shop", "m2")
    ctx.cluster.shop.admin("outage", {"shard": shard, "seconds": 60})  # 长时间隔离
    res = ctx.gateway.order("u2", "m2", "s1", qty=1, amount=30)
    assert res.retrying and res.state == "INIT", res
    tid = res.txn_id
    print(f"  下单提交：店家 m2 持续不可达，事务滞留 INIT，指数退避重试...")

    deadline = time.time() + 5
    attempts = 0
    while time.time() < deadline:
        ctx.poller.tick()
        rec = ctx.executor.txn.get(tid)
        if rec and rec["state"] == "MANUAL_REVIEW":
            break
        time.sleep(0.1)
        attempts += 1

    rec = ctx.executor.txn.get(tid)
    print(f"  终态：{rec['state']}（attempts={rec['attempts']}，最大={ctx.cfg.max_attempts}）")
    assert rec["state"] == "MANUAL_REVIEW", rec
    assert rec["attempts"] >= ctx.cfg.max_attempts

    manual = [a for a in ctx.alerts() if a["kind"] == "manual_review" and a["txn_id"] == tid]
    assert manual, "应触发人工复核告警"
    print(f"  告警: {manual[0]['message']}")
    print("  已停止无限重试，转入 MANUAL_REVIEW，等待人工复核。")

    ctx.cluster.shop.admin("outage", {"shard": shard, "clear": True})  # 恢复，演示结束
    ctx.results.append(("案例7 重试耗尽人工复核", True, "退避达上限 -> MANUAL_REVIEW + 告警"))
    return True
