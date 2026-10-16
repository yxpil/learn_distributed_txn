# -*- coding: utf-8 -*-
"""
协调器进程：负责拉起 / 重启 / 关闭三个 node 服务器子进程，
并装配框架组件（客户端、执行器、轮询器、死信、网关、对账），跑案例。
"""
import hashlib
import os
import subprocess
import sys
import time

from framework import config
from framework.client.typed_clients import UserClient, ShopClient, TxnClient
from framework.core.executor import TxnExecutor
from framework.core.state_machine import describe as sm_describe
from framework.gateway import Gateway
from framework.scheduler.deadletter import DeadLetter
from framework.scheduler.poller import Poller
from framework.reconcile.offline_reconcile import OfflineReconciler
from framework.proto import SUCCESS, ROLLED_BACK, MANUAL_REVIEW, INIT, FROZEN, ROLLBACK

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

NODE_ENTRY = os.path.join(ROOT, "node_entry.py")


def shard_name(kind, key, n):
    h = int(hashlib.md5(str(key).encode()).hexdigest(), 16) % n
    return f"shard_{h}.json"


class Cluster:
    """管理三个 node 进程。"""

    def __init__(self, data_dir, user_port=None, shop_port=None, txn_port=None):
        self.data_dir = data_dir
        self.user_port = user_port or config.USER_NODE_PORT
        self.shop_port = shop_port or config.SHOP_NODE_PORT
        self.txn_port = txn_port or config.TXN_NODE_PORT
        self.user_url = f"http://127.0.0.1:{self.user_port}"
        self.shop_url = f"http://127.0.0.1:{self.shop_port}"
        self.txn_url = f"http://127.0.0.1:{self.txn_port}"
        self._procs = {}
        self.user = UserClient(self.user_url)
        self.shop = ShopClient(self.shop_url)
        self.txn = TxnClient(self.txn_url)
        self.log_dir = os.path.join(data_dir, "_logs")
        os.makedirs(self.log_dir, exist_ok=True)

    # ---------- 进程管理 ----------
    def _spawn(self, role, port, data_sub):
        env = dict(os.environ)
        if role in ("user", "shop"):
            env["TXN_NODE_URL"] = self.txn_url
        logf = open(os.path.join(self.log_dir, f"{role}.log"), "a", encoding="utf-8")
        proc = subprocess.Popen(
            [sys.executable, NODE_ENTRY, role, str(port), data_sub],
            cwd=ROOT, env=env, stdout=logf, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
        self._procs[role] = proc
        return proc

    def start_all(self, clean=True):
        if clean:
            for sub in ("user", "shop", "txn"):
                import shutil
                d = os.path.join(self.data_dir, sub)
                if os.path.isdir(d):
                    shutil.rmtree(d, ignore_errors=True)
                os.makedirs(d, exist_ok=True)
        self._spawn("txn", self.txn_port, os.path.join(self.data_dir, "txn"))
        self._spawn("user", self.user_port, os.path.join(self.data_dir, "user"))
        self._spawn("shop", self.shop_port, os.path.join(self.data_dir, "shop"))
        self.wait_all()

    def restart(self, role):
        """终止并重启某个节点进程（数据目录保留，用于模拟宕机恢复）。"""
        old = self._procs.pop(role, None)
        if old:
            old.terminate()
            try:
                old.wait(timeout=5)
            except subprocess.TimeoutExpired:
                old.kill()
        data_sub = os.path.join(self.data_dir, role)
        port = {"user": self.user_port, "shop": self.shop_port, "txn": self.txn_port}[role]
        self._spawn(role, port, data_sub)
        self.wait_health(role)

    def stop(self, role):
        proc = self._procs.pop(role, None)
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def shutdown(self):
        for role in list(self._procs):
            self.stop(role)

    # ---------- 探活 ----------
    def _client_of(self, role):
        return {"user": self.user, "shop": self.shop, "txn": self.txn}[role]

    def wait_health(self, role, timeout=20):
        client = self._client_of(role)
        t0 = time.time()
        while time.time() - t0 < timeout:
            h = client._c.health()
            if h and h.get("data", {}).get("status") == "up":
                return h
            time.sleep(0.15)
        raise TimeoutError(f"节点 {role} 未能就绪")

    def wait_all(self):
        for role in ("txn", "user", "shop"):
            self.wait_health(role)

    # ---------- 种子数据 ----------
    def seed(self, users=None, shops=None):
        users = users or {"u1": 100, "u2": 500, "u_low": 10}
        shops = shops or {"m1": {"s1": 10, "s2": 5}, "m2": {"s1": 3}}
        self.user.seed(users)
        self.shop.seed(shops)
        return users, shops


def build_framework(cluster, on_transition=None, base_backoff=None,
                    max_attempts=None, freeze_ttl=None):
    executor = TxnExecutor(cluster.user, cluster.shop, cluster.txn,
                           on_transition=on_transition,
                           base_backoff=base_backoff, max_attempts=max_attempts,
                           freeze_ttl=freeze_ttl)
    gateway = Gateway(executor)
    dlq = DeadLetter(executor, cluster.user, cluster.shop)
    poller = Poller(cluster.txn, dlq, interval=0.05)
    reconciler = OfflineReconciler(cluster.txn, cluster.user, cluster.shop)
    return executor, gateway, dlq, poller, reconciler


def balance(user_client, user_id):
    return (user_client.get(user_id) or {}).get("balance")


def stock(shop_client, merchant_id, sku):
    sk = shop_client._c.call("shop_get_sku", {"merchant_id": merchant_id, "sku": sku}) or {}
    return sk.get("stock")


# 状态流转打印器（给案例用的 on_transition）
def make_transition_printer():
    transitions = []

    def printer(txn_id, src, dst, rec):
        if src is None:
            transitions.append(f"    {dst:<14} 创建事务 {txn_id[:8]}")
        else:
            transitions.append(f"    {src:<14} -> {dst:<14} {txn_id[:8]}")
        return transitions

    return transitions, printer
