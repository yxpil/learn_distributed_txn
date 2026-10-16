# -*- coding: utf-8 -*-
"""面向执行器的三个类型化客户端。"""
from ..proto import (
    CMD_SEED_USER, CMD_USER_FREEZE, CMD_USER_COMMIT, CMD_USER_REFUND,
    CMD_USER_UNFREEZE, CMD_USER_GET, CMD_USER_MARKERS, CMD_USER_SNAPSHOT,
    CMD_SEED_SHOP, CMD_SHOP_FREEZE, CMD_SHOP_COMMIT, CMD_SHOP_ADD_BACK,
    CMD_SHOP_UNFREEZE, CMD_SHOP_MARKERS, CMD_SHOP_SNAPSHOT, CMD_SHOP_GET_SKU,
    CMD_TXN_CREATE, CMD_TXN_UPDATE, CMD_TXN_GET, CMD_TXN_ALL,
    CMD_TXN_NON_TERMINAL, CMD_TXN_SNAPSHOT_BACKUP, CMD_TXN_WIPE,
    CMD_TXN_RESTORE_BACKUP,
)
from .node_client import NodeClient


class UserClient:
    def __init__(self, base_url, timeout=3.0):
        self._c = NodeClient(base_url, "user_node", timeout)

    def seed(self, users):
        return self._c.call(CMD_SEED_USER, {"users": users})

    def get(self, user_id):
        return self._c.call(CMD_USER_GET, {"user_id": user_id})

    def freeze(self, user_id, txn_id, amount, expire_at):
        return self._c.call(CMD_USER_FREEZE, {
            "user_id": user_id, "txn_id": txn_id, "amount": amount,
            "expire_at": expire_at})

    def commit(self, user_id, txn_id, amount):
        return self._c.call(CMD_USER_COMMIT, {
            "user_id": user_id, "txn_id": txn_id, "amount": amount})

    def refund(self, user_id, txn_id, amount):
        return self._c.call(CMD_USER_REFUND, {
            "user_id": user_id, "txn_id": txn_id, "amount": amount})

    def unfreeze(self, user_id, txn_id):
        return self._c.call(CMD_USER_UNFREEZE, {"user_id": user_id, "txn_id": txn_id})

    def markers(self):
        return self._c.call(CMD_USER_MARKERS)

    def snapshot(self):
        return self._c.call(CMD_USER_SNAPSHOT)

    def admin(self, action, payload=None):
        return self._c.admin(action, payload)


class ShopClient:
    def __init__(self, base_url, timeout=3.0):
        self._c = NodeClient(base_url, "shop_node", timeout)

    def seed(self, merchants):
        return self._c.call(CMD_SEED_SHOP, {"merchants": merchants})

    def freeze(self, merchant_id, sku, txn_id, qty, expire_at):
        return self._c.call(CMD_SHOP_FREEZE, {
            "merchant_id": merchant_id, "sku": sku, "txn_id": txn_id,
            "qty": qty, "expire_at": expire_at})

    def commit(self, merchant_id, sku, txn_id, qty):
        return self._c.call(CMD_SHOP_COMMIT, {
            "merchant_id": merchant_id, "sku": sku, "txn_id": txn_id, "qty": qty})

    def add_back(self, merchant_id, sku, txn_id, qty):
        return self._c.call(CMD_SHOP_ADD_BACK, {
            "merchant_id": merchant_id, "sku": sku, "txn_id": txn_id, "qty": qty})

    def unfreeze(self, merchant_id, sku, txn_id):
        return self._c.call(CMD_SHOP_UNFREEZE, {
            "merchant_id": merchant_id, "sku": sku, "txn_id": txn_id})

    def markers(self):
        return self._c.call(CMD_SHOP_MARKERS)

    def snapshot(self):
        return self._c.call(CMD_SHOP_SNAPSHOT)

    def admin(self, action, payload=None):
        return self._c.admin(action, payload)


class TxnClient:
    def __init__(self, base_url, timeout=3.0):
        self._c = NodeClient(base_url, "txn_node", timeout)

    def create(self, record):
        return self._c.call(CMD_TXN_CREATE, {"record": record})

    def update(self, txn_id, **fields):
        return self._c.call(CMD_TXN_UPDATE, {"txn_id": txn_id, "fields": fields})

    def get(self, txn_id):
        return self._c.call(CMD_TXN_GET, {"txn_id": txn_id})

    def all(self):
        return self._c.call(CMD_TXN_ALL)

    def non_terminal(self):
        return self._c.call(CMD_TXN_NON_TERMINAL)

    def snapshot_backup(self):
        return self._c.call(CMD_TXN_SNAPSHOT_BACKUP)

    def wipe(self):
        return self._c.call(CMD_TXN_WIPE)

    def restore_backup(self, newest=True):
        return self._c.call(CMD_TXN_RESTORE_BACKUP, {"newest": newest})

    def admin(self, action, payload=None):
        return self._c.admin(action, payload)
