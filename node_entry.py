# -*- coding: utf-8 -*-
"""node 服务器进程入口：
    python node_entry.py <role: user|shop|txn> <port> <data_dir> [--shards N] [--replicas N]
自检插件需要事务中心地址时，通过环境变量 TXN_NODE_URL 传入。
"""
import argparse
import os
import sys

from framework.node.server import run_server

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("role", choices=["user", "shop", "txn"])
    p.add_argument("port", type=int)
    p.add_argument("data_dir")
    p.add_argument("--shards", type=int, default=None)
    p.add_argument("--replicas", type=int, default=None)
    args = p.parse_args()

    txn_url = os.environ.get("TXN_NODE_URL")
    run_server(args.role, args.port, args.data_dir,
               txn_url=txn_url, shards=args.shards, replicas=args.replicas)


if __name__ == "__main__":
    main()
