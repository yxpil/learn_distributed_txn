# -*- coding: utf-8 -*-
"""
一键运行：拉起 3 个 node 服务器进程（user / shop / txn），
装配框架并依次跑 7 个演示案例，最后统一收尾。

用法：  python run_all.py
"""
import os
import sys
import time
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from framework import config
from harness import (Cluster, build_framework, make_transition_printer,
                     shard_name, balance, stock)
from cases import (
    case1_normal, case2_freeze_fail, case3_commit_fail_refund,
    case4_network_recover, case5_node_selfcheck, case6_reconcile_orphan,
    case7_retry_exhaust,
)

DATA_DIR = os.path.join(ROOT, "data")


def main():
    config.ALERTS.clear()
    print("=" * 64)
    print("防止脑裂分布式事务框架 —— 三个 node 服务器进程级模拟")
    print("=" * 64)

    # ---- 启动三个节点进程 ----
    cluster = Cluster(DATA_DIR)
    print("\n[启动] 拉起 txn / user / shop 三个 node 服务器进程 ...")
    cluster.start_all(clean=True)
    print(f"        user -> {cluster.user_url}   shop -> {cluster.shop_url}   txn -> {cluster.txn_url}")

    # ---- 装配框架 ----
    transitions, printer = make_transition_printer()
    executor, gateway, dlq, poller, reconciler = build_framework(cluster, on_transition=printer)
    cluster.seed()
    print("\n[种子] 用户 u1=100 / u2=500 / u_low=10；商家 m1{s1:10,s2:5} / m2{s1:3}")

    from framework.core.state_machine import describe as sm_describe
    print("\n" + sm_describe() + "\n")

    # ---- 给案例的上下文 ----
    ctx = SimpleNamespace(
        cluster=cluster, executor=executor, gateway=gateway, dlq=dlq,
        poller=poller, reconciler=reconciler,
        user=cluster.user, shop=cluster.shop, txn=cluster.txn,
        transitions=lambda: list(transitions),
        alerts=lambda: list(config.ALERTS),
        balance=lambda u: balance(cluster.user, u),
        stock=lambda m, s: stock(cluster.shop, m, s),
        shard=lambda kind, key: shard_name(kind, key, {"user": config.USER_SHARDS, "shop": config.SHOP_SHARDS}[kind]),
        cfg=SimpleNamespace(max_attempts=config.MAX_ATTEMPTS, base_backoff=config.BASE_BACKOFF),
        results=[],
    )

    cases = [
        case1_normal.run,
        case2_freeze_fail.run,
        case3_commit_fail_refund.run,
        case4_network_recover.run,
        case5_node_selfcheck.run,
        case6_reconcile_orphan.run,
        case7_retry_exhaust.run,
    ]

    for fn in cases:
        name = fn.__module__.split(".")[-1]
        transitions.clear()  # 每个案例只展示自己的状态流转
        try:
            ok = fn(ctx)
            if ok:
                print(f"  [PASS] {name}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            ctx.results.append((name, False, str(e)))
            print(f"  [FAIL] {name}: {e}")

    # ---- 汇总 ----
    print("\n" + "=" * 64)
    print("运行结果汇总")
    print("=" * 64)
    all_ok = True
    for name, ok, note in ctx.results:
        all_ok = all_ok and ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}  -  {note}")

    print("\n本次产生的告警：")
    for a in config.ALERTS:
        print(f"  [{a['kind']}] {a['message']}")
    if not config.ALERTS:
        print("  （无）")

    cluster.shutdown()
    print("\n三个 node 进程已关闭。")
    print("总体结果:", "全部通过" if all_ok else "存在失败")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
