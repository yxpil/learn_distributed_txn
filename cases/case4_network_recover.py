# -*- coding: utf-8 -*-
"""案例4：店家节点网络隔离 -> 事务滞留 INIT -> 节点恢复 -> 轮询器+死信重连续做直至 SUCCESS。"""
import time


def run(ctx):
    print("\n" + "=" * 64)
    print("案例4 网络恢复：店家分片隔离 -> 滞留 INIT -> 恢复后由轮询器/死信重连续做")
    print("=" * 64)

    b0 = ctx.balance("u2")
    s0 = ctx.stock("m1", "s1")
    shard = ctx.shard("shop", "m1")

    # 店家 m1 所在分片进入隔离
    ctx.cluster.shop.admin("outage", {"shard": shard, "seconds": 30})
    res = ctx.gateway.order("u2", "m1", "s1", qty=1, amount=30)
    assert res.retrying and res.state == "INIT", res
    tid = res.txn_id
    print(f"  下单提交：店家不可达，事务滞留 INIT（第{ctx.executor.txn.get(tid)['attempts']}次尝试），"
          f"下次重试={ctx.executor.txn.get(tid)['next_retry_at']:.2f}")

    # 恢复店家分片
    ctx.cluster.shop.admin("outage", {"shard": shard, "clear": True})
    print("  店家分片恢复，轮询器开始扫描非终态事务...")

    # 驱动轮询器直到终态
    deadline = time.time() + 5
    while time.time() < deadline:
        ctx.poller.tick()
        rec = ctx.executor.txn.get(tid)
        if rec and rec["state"] in ("SUCCESS", "ROLLED_BACK", "MANUAL_REVIEW"):
            break
        time.sleep(0.1)

    rec = ctx.executor.txn.get(tid)
    print("  状态流转：")
    for line in ctx.transitions():
        print(line)
    print(f"  终态：{rec['state']}（attempts={rec['attempts']}）")
    assert rec["state"] == "SUCCESS", rec

    b1 = ctx.balance("u2")
    s1 = ctx.stock("m1", "s1")
    print(f"  结果：余额={b1}（应 {b0-30}），库存={s1}（应 {s0-1}）")
    assert b1 == b0 - 30
    assert s1 == s0 - 1

    ctx.results.append(("案例4 网络恢复续做", True, "轮询器+死信重连后 SUCCESS"))
    return True
