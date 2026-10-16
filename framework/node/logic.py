# -*- coding: utf-8 -*-
"""
节点内的"数据库引擎"：每个 node 服务器持有一种数据库的读写逻辑。
- 用户资产库（UserNodeLogic）：用户ID水平分片，资产 = 余额 + 冻结标记
- 店家库存库（ShopNodeLogic）：按商家分库，库存 = 现货 + 冻结标记
- 全局事务状态库（TxnNodeLogic）：多副本 + 定时快照备份
所有操作都是"单库本地事务"（文件锁 + 原子 JSON 替换），不做跨库协调。
分片级宕机用 outage 表模拟；进程级宕机则直接停掉整个服务器进程。
"""
import hashlib
import json
import os
import time

from ..errors import BusinessFailure, DbUnavailable
from .json_store import JsonStore


class NodeLogicBase:
    ROLE = "base"

    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)
        self._outage = {}          # shard -> until_ts

    # ---------- 宕机模拟（分片级） ----------
    def is_down(self, shard, now=None):
        now = now or time.time()
        until = self._outage.get(shard)
        if until is None:
            return False
        if until > now:
            return True
        self._outage.pop(shard, None)
        return False

    def mark_outage(self, shard, seconds):
        self._outage[shard] = time.time() + seconds

    def clear_outage(self, shard):
        self._outage.pop(shard, None)

    def outage_state(self):
        return {k: v for k, v in self._outage.items() if v > time.time()}

    def _guard(self, shard):
        if self.is_down(shard):
            raise DbUnavailable(f"{self.ROLE}[{shard}] 节点不可达/隔离")

    def _flow(self, op, txn_id, amount):
        return {"ts": time.time(), "op": op, "txn_id": txn_id, "amount": amount}

    # 子类实现
    def dispatch(self, cmd, payload):
        raise NotImplementedError

    def snapshot(self):
        raise NotImplementedError


class UserNodeLogic(NodeLogicBase):
    """用户资产库：按 user_id 水平分片。"""

    ROLE = "user_db"

    def __init__(self, root, shards=4):
        super().__init__(root)
        self.shards = shards
        self._stores = {}

    def _shard_file(self, user_id):
        n = int(hashlib.md5(str(user_id).encode()).hexdigest(), 16) % self.shards
        return os.path.join(self.root, f"shard_{n}.json")

    def _shard_name(self, user_id):
        return os.path.basename(self._shard_file(user_id))

    def _store(self, user_id):
        p = self._shard_file(user_id)
        if p not in self._stores:
            self._stores[p] = JsonStore(p)
        return self._stores[p]

    # ---------- 操作 ----------
    def seed(self, users):
        """users: {uid: balance}"""
        for uid, bal in users.items():
            self._store(uid).local_tx(
                lambda d, _u=uid, _b=bal: d.setdefault(
                    "users", {}).setdefault(
                    _u, {"balance": _b, "frozen": {}, "flows": []}) or d)
        return {"seeded": len(users)}

    def get(self, user_id):
        self._guard(self._shard_name(user_id))
        d = self._store(user_id).read()
        return d.get("users", {}).get(user_id)

    def available(self, user_id):
        u = self.get(user_id) or {}
        frozen_sum = sum(f["amount"] for f in (u.get("frozen") or {}).values())
        return (u.get("balance") or 0) - frozen_sum

    def freeze(self, user_id, txn_id, amount, expire_at):
        shard = self._shard_name(user_id)
        self._guard(shard)

        def do(d):
            users = d.setdefault("users", {})
            u = users.get(user_id)
            if u is None:
                raise BusinessFailure(f"用户 {user_id} 不存在")
            if txn_id in u.setdefault("frozen", {}):
                return True  # 幂等：同一事务已冻结
            frozen_sum = sum(f["amount"] for f in u["frozen"].values())
            if u["balance"] - frozen_sum < amount:
                raise BusinessFailure(
                    f"用户 {user_id} 可用余额不足（需 {amount}，可用 {u['balance'] - frozen_sum}）")
            u["frozen"][txn_id] = {"amount": amount, "expire_at": expire_at}
            u.setdefault("flows", []).append(self._flow("freeze", txn_id, amount))
            return True
        return self._store(user_id).local_tx(do)

    def commit(self, user_id, txn_id, amount):
        shard = self._shard_name(user_id)
        self._guard(shard)

        def do(d):
            u = d["users"].get(user_id)
            if u is None:
                raise BusinessFailure(f"用户 {user_id} 不存在")
            f = u.get("frozen", {}).get(txn_id)
            if f is None:
                raise BusinessFailure(f"用户 {user_id} 无 {txn_id} 冻结标记，无法扣减")
            u["balance"] -= amount
            del u["frozen"][txn_id]
            u.setdefault("flows", []).append(self._flow("commit_deduct", txn_id, amount))
            return True
        return self._store(user_id).local_tx(do)

    def refund(self, user_id, txn_id, amount):
        """补偿：已扣真实余额的，把余额加回来并清标记。"""
        shard = self._shard_name(user_id)
        self._guard(shard)

        def do(d):
            u = d["users"].get(user_id)
            if u is None:
                raise BusinessFailure(f"用户 {user_id} 不存在")
            u["balance"] += amount
            u.setdefault("frozen", {}).pop(txn_id, None)
            u.setdefault("flows", []).append(self._flow("refund", txn_id, amount))
            return True
        return self._store(user_id).local_tx(do)

    def unfreeze(self, user_id, txn_id):
        """补偿：只解冻，不动真实余额。"""
        shard = self._shard_name(user_id)
        self._guard(shard)

        def do(d):
            u = d["users"].get(user_id)
            if u is None:
                return True
            if txn_id in u.setdefault("frozen", {}):
                del u["frozen"][txn_id]
                u.setdefault("flows", []).append(self._flow("unfreeze", txn_id, 0))
            return True
        return self._store(user_id).local_tx(do)

    def all_frozen_markers(self):
        """返回 {txn_id: [(user_id, amount, expire_at)]} —— 供自检/对账。"""
        out = {}
        for i in range(self.shards):
            p = os.path.join(self.root, f"shard_{i}.json")
            if not os.path.exists(p):
                continue
            d = JsonStore(p).read()
            for uid, u in (d.get("users") or {}).items():
                for txn_id, f in (u.get("frozen") or {}).items():
                    out.setdefault(txn_id, []).append(
                        {"node": self.ROLE, "user_id": uid, "amount": f["amount"],
                         "expire_at": f["expire_at"]})
        return out

    def snapshot(self):
        out = {"role": self.ROLE, "shards": {}}
        for i in range(self.shards):
            p = os.path.join(self.root, f"shard_{i}.json")
            out["shards"][f"shard_{i}.json"] = JsonStore(p).read() if os.path.exists(p) else {}
        return out

    def dispatch(self, cmd, payload):
        if cmd == "seed_user":
            return self.seed(payload.get("users", {}))
        if cmd == "user_get":
            return self.get(payload["user_id"])
        if cmd == "user_freeze":
            return self.freeze(payload["user_id"], payload["txn_id"],
                               payload["amount"], payload["expire_at"])
        if cmd == "user_commit":
            return self.commit(payload["user_id"], payload["txn_id"], payload["amount"])
        if cmd == "user_refund":
            return self.refund(payload["user_id"], payload["txn_id"], payload["amount"])
        if cmd == "user_unfreeze":
            return self.unfreeze(payload["user_id"], payload["txn_id"])
        if cmd == "user_markers":
            return self.all_frozen_markers()
        if cmd == "user_snapshot":
            return self.snapshot()
        raise KeyError(f"未知命令: {cmd}")


class ShopNodeLogic(NodeLogicBase):
    """店家库存库：按商家分库。"""

    ROLE = "shop_db"

    def __init__(self, root, shards=2):
        super().__init__(root)
        self.shards = shards
        self._stores = {}
        self.fail_commit_once = None  # 运维注入：下一次 shop_commit 抛异常

    def _shard_file(self, merchant_id):
        n = int(hashlib.md5(str(merchant_id).encode()).hexdigest(), 16) % self.shards
        return os.path.join(self.root, f"shard_{n}.json")

    def _shard_name(self, merchant_id):
        return os.path.basename(self._shard_file(merchant_id))

    def _store(self, merchant_id):
        p = self._shard_file(merchant_id)
        if p not in self._stores:
            self._stores[p] = JsonStore(p)
        return self._stores[p]

    def seed(self, merchants):
        """merchants: {mid: {sku: stock}}"""
        for mid, skus in merchants.items():
            self._store(mid).local_tx(
                lambda d, _m=mid, _s=skus: d.setdefault(
                    "merchants", {}).setdefault(
                    _m, {"skus": {k: {"stock": v, "frozen": {}} for k, v in _s.items()}}) or d)
        return {"seeded": len(merchants)}

    def get_sku(self, merchant_id, sku):
        self._guard(self._shard_name(merchant_id))
        d = self._store(merchant_id).read()
        return (d.get("merchants") or {}).get(merchant_id, {}).get("skus", {}).get(sku)

    def _get_merchant(self, merchant_id):
        self._guard(self._shard_name(merchant_id))
        d = self._store(merchant_id).read()
        return (d.get("merchants") or {}).get(merchant_id)

    def freeze(self, merchant_id, sku, txn_id, qty, expire_at):
        shard = self._shard_name(merchant_id)
        self._guard(shard)

        def do(d):
            m = (d.setdefault("merchants", {})).get(merchant_id)
            if m is None:
                raise BusinessFailure(f"商家 {merchant_id} 不存在")
            sk = m["skus"].get(sku)
            if sk is None:
                raise BusinessFailure(f"商家 {merchant_id} 无 SKU {sku}")
            if txn_id in sk.setdefault("frozen", {}):
                return True
            frozen_sum = sum(f["qty"] for f in sk["frozen"].values())
            if sk["stock"] - frozen_sum < qty:
                raise BusinessFailure(
                    f"商家 {merchant_id}/{sku} 可用库存不足（需 {qty}，可用 {sk['stock'] - frozen_sum}）")
            sk["frozen"][txn_id] = {"qty": qty, "expire_at": expire_at}
            return True
        return self._store(merchant_id).local_tx(do)

    def commit(self, merchant_id, sku, txn_id, qty):
        if self.fail_commit_once:  # 运维注入：模拟下游 COMMIT 阶段异常
            mode = self.fail_commit_once
            self.fail_commit_once = None
            if mode == "unavailable":
                raise DbUnavailable(f"shop[{merchant_id}] COMMIT 阶段瞬时故障(注入)")
            if mode == "business":
                raise BusinessFailure(f"shop[{merchant_id}] COMMIT 被下游拒绝(注入)")
        shard = self._shard_name(merchant_id)
        self._guard(shard)

        def do(d):
            sk = d["merchants"][merchant_id]["skus"].get(sku)
            f = (sk or {}).get("frozen", {}).get(txn_id)
            if f is None:
                raise BusinessFailure(f"商家 {merchant_id}/{sku} 无 {txn_id} 冻结标记")
            sk["stock"] -= qty
            del sk["frozen"][txn_id]
            return True
        return self._store(merchant_id).local_tx(do)

    def add_back(self, merchant_id, sku, txn_id, qty):
        """补偿：已扣库存的，加回并清标记。"""
        shard = self._shard_name(merchant_id)
        self._guard(shard)

        def do(d):
            sk = d["merchants"][merchant_id]["skus"].get(sku)
            if sk is None:
                raise BusinessFailure(f"商家 {merchant_id}/{sku} 不存在")
            sk["stock"] += qty
            sk.setdefault("frozen", {}).pop(txn_id, None)
            return True
        return self._store(merchant_id).local_tx(do)

    def unfreeze(self, merchant_id, sku, txn_id):
        """补偿：只解冻，不动真实库存。"""
        shard = self._shard_name(merchant_id)
        self._guard(shard)

        def do(d):
            m = d.get("merchants", {}).get(merchant_id)
            if not m:
                return True
            sk = m["skus"].get(sku)
            if sk and txn_id in sk.setdefault("frozen", {}):
                del sk["frozen"][txn_id]
            return True
        return self._store(merchant_id).local_tx(do)

    def all_frozen_markers(self):
        out = {}
        for i in range(self.shards):
            p = os.path.join(self.root, f"shard_{i}.json")
            if not os.path.exists(p):
                continue
            d = JsonStore(p).read()
            for mid, m in (d.get("merchants") or {}).items():
                for sku, sk in m.get("skus", {}).items():
                    for txn_id, f in (sk.get("frozen") or {}).items():
                        out.setdefault(txn_id, []).append(
                            {"node": self.ROLE, "merchant_id": mid, "sku": sku,
                             "qty": f["qty"], "expire_at": f["expire_at"]})
        return out

    def snapshot(self):
        out = {"role": self.ROLE, "shards": {}}
        for i in range(self.shards):
            p = os.path.join(self.root, f"shard_{i}.json")
            out["shards"][f"shard_{i}.json"] = JsonStore(p).read() if os.path.exists(p) else {}
        return out

    def dispatch(self, cmd, payload):
        if cmd == "seed_shop":
            return self.seed(payload.get("merchants", {}))
        if cmd == "shop_freeze":
            return self.freeze(payload["merchant_id"], payload["sku"], payload["txn_id"],
                               payload["qty"], payload["expire_at"])
        if cmd == "shop_commit":
            return self.commit(payload["merchant_id"], payload["sku"], payload["txn_id"], payload["qty"])
        if cmd == "shop_add_back":
            return self.add_back(payload["merchant_id"], payload["sku"], payload["txn_id"], payload["qty"])
        if cmd == "shop_unfreeze":
            return self.unfreeze(payload["merchant_id"], payload["sku"], payload["txn_id"])
        if cmd == "shop_markers":
            return self.all_frozen_markers()
        if cmd == "shop_snapshot":
            return self.snapshot()
        if cmd == "shop_get_sku":
            return self.get_sku(payload["merchant_id"], payload["sku"])
        raise KeyError(f"未知命令: {cmd}")


class TxnNodeLogic(NodeLogicBase):
    """全局事务状态库：多副本 + 定时快照备份。"""

    ROLE = "txn_db"

    def __init__(self, root, replicas=2):
        super().__init__(root)
        self.replicas = replicas
        self.main = JsonStore(os.path.join(root, "txns.json"), initial={"txns": {}})
        self._replica_stores = [
            JsonStore(os.path.join(root, f"replica_{i}", "txns.json"), initial={"txns": {}})
            for i in range(replicas)
        ]
        self.backup_dir = os.path.join(root, "backup")
        os.makedirs(self.backup_dir, exist_ok=True)

    def _persist(self, txns):
        def apply(d):
            d.clear()
            d["txns"] = txns
        self.main.local_tx(apply)
        for r in self._replica_stores:
            r.local_tx(apply)
        return txns

    def create(self, record):
        data = self.main.read()
        txns = data.setdefault("txns", {})
        txns[record["txn_id"]] = record
        return self._persist(txns)

    def update(self, txn_id, **fields):
        data = self.main.read()
        txns = data.setdefault("txns", {})
        if txn_id not in txns:
            raise KeyError(f"事务不存在: {txn_id}")
        txns[txn_id].update(fields)
        return self._persist(txns)

    def get(self, txn_id):
        return self.main.read().get("txns", {}).get(txn_id)

    def all(self):
        return self.main.read().get("txns", {})

    def non_terminal(self):
        from ..proto import TERMINAL
        txns = self.all()
        return {k: v for k, v in txns.items() if v.get("state") not in TERMINAL}

    def snapshot_backup(self):
        data = self.main.read()
        path = os.path.join(self.backup_dir, f"snapshot_{int(time.time()*1000)}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return {"backup": path, "txn_count": len(data.get("txns", {}))}

    def wipe(self):
        """仅演示：模拟全局状态库主数据彻底损毁（副本/备份仍可救）。"""
        return self._persist({})

    def restore_backup(self, newest=True):
        files = sorted(
            os.path.join(self.backup_dir, n) for n in os.listdir(self.backup_dir)
            if n.startswith("snapshot_"))
        if not files:
            raise BusinessFailure("无可用备份")
        path = files[-1] if newest else files[0]
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"restored": path, "txn_count": len(data.get("txns", {})), "data": data}

    def dispatch(self, cmd, payload):
        if cmd == "txn_create":
            return self.create(payload["record"])
        if cmd == "txn_update":
            return self.update(payload["txn_id"], **payload.get("fields", {}))
        if cmd == "txn_get":
            return self.get(payload["txn_id"])
        if cmd == "txn_all":
            return self.all()
        if cmd == "txn_non_terminal":
            return self.non_terminal()
        if cmd == "txn_snapshot_backup":
            return self.snapshot_backup()
        if cmd == "txn_wipe":
            return self.wipe()
        if cmd == "txn_restore_backup":
            return self.restore_backup(payload.get("newest", True))
        raise KeyError(f"未知命令: {cmd}")


def build_logic(role, root, shards=None, replicas=None):
    role = role.lower()
    if role in ("user", "user_db"):
        return UserNodeLogic(root, shards or 4)
    if role in ("shop", "shop_db"):
        return ShopNodeLogic(root, shards or 2)
    if role in ("txn", "txn_db"):
        return TxnNodeLogic(root, replicas or 2)
    raise ValueError(f"未知节点角色: {role}")
