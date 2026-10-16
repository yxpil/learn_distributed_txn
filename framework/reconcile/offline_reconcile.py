# -*- coding: utf-8 -*-
"""兜底系统：离线对账模块。
定时扫描 用户资产流水 / 店家库存流水 / 全局事务单据，三者交叉校验：
- 检出孤儿冻结（本地有冻结标记，但找不到对应全局事务）-> 自动解冻 + 告警
- 终态事务残留冻结标记 -> 清理 + 告警
- 全局事务状态库主数据丢失但有备份 -> 提示可从备份恢复
"""
import time

from ..errors import DbUnavailable
from ..proto import SUCCESS, ROLLED_BACK, TERMINAL
from .. import config


class OfflineReconciler:
    def __init__(self, txn, user, shop, auto_fix=True):
        self.txn = txn
        self.user = user
        self.shop = shop
        self.auto_fix = auto_fix

    def run(self):
        report = {
            "scanned_at": time.time(),
            "orphan_freezes": [],      # 孤儿冻结：有标记无单据
            "leftover_markers": [],    # 终态却残留标记
            "missing_records": [],     # 有单据参考但主库缺失（备份可救）
            "inconsistencies": [],
            "fixed": [],
        }

        user_markers = self.user.markers()
        shop_markers = self.shop.markers()
        try:
            txns = self.txn.all()
        except DbUnavailable:
            txns = {}

        # ---------- 1) 孤儿冻结：有标记，但全局无对应事务 ----------
        for txn_id, markers in user_markers.items():
            if txn_id not in txns:
                report["orphan_freezes"].append({"node": "user", "txn_id": txn_id, "markers": markers})
                if self.auto_fix:
                    for m in markers:
                        self.user.unfreeze(m["user_id"], txn_id)
                    report["fixed"].append(f"user 解冻孤儿 {txn_id}")
                    config.alert("orphan_unfreeze", f"对账：解冻孤儿冻结 user/{txn_id}", txn_id)

        for txn_id, markers in shop_markers.items():
            if txn_id not in txns:
                report["orphan_freezes"].append({"node": "shop", "txn_id": txn_id, "markers": markers})
                if self.auto_fix:
                    for m in markers:
                        self.shop.unfreeze(m["merchant_id"], m["sku"], txn_id)
                    report["fixed"].append(f"shop 解冻孤儿 {txn_id}")
                    config.alert("orphan_unfreeze", f"对账：解冻孤儿冻结 shop/{txn_id}", txn_id)

        # ---------- 2) 终态事务残留冻结标记 ----------
        for txn_id, markers in {**user_markers, **shop_markers}.items():
            rec = txns.get(txn_id)
            if rec and rec["state"] in TERMINAL:
                for m in markers:
                    report["leftover_markers"].append({"txn_id": txn_id, "state": rec["state"], "marker": m})
                    if self.auto_fix:
                        if "user_id" in m:
                            self.user.unfreeze(m["user_id"], txn_id)
                        else:
                            self.shop.unfreeze(m["merchant_id"], m["sku"], txn_id)
                        report["fixed"].append(f"清理终态残留 {txn_id}")
                        config.alert("leftover_marker", f"对账：清理终态残留标记 {txn_id}", txn_id)

        # ---------- 3) 事务单据交叉：SUCCESS/ROLLED_BACK 是否都清干净了 ----------
        for txn_id, rec in txns.items():
            if rec["state"] in (SUCCESS, ROLLED_BACK):
                still = [m for m in user_markers.get(txn_id, [])]
                still += [m for m in shop_markers.get(txn_id, [])]
                if still:
                    report["inconsistencies"].append(
                        {"txn_id": txn_id, "state": rec["state"], "left_markers": len(still)})

        # ---------- 4) 全局状态库健康提示 ----------
        if not txns:
            report["missing_records"].append("全局事务状态库主数据为空，疑似丢失，可用备份恢复")

        return report
