# -*- coding: utf-8 -*-
"""第一层自愈（上半）：全局状态轮询器。
常驻扫描所有非终态事务，到期的交给死信处理器。"""
import time


class Poller:
    def __init__(self, txn, deadletter, interval=0.2):
        self.txn = txn
        self.deadletter = deadletter
        self.interval = interval

    def due_txns(self, now=None):
        now = now or time.time()
        out = []
        for txn_id, rec in self.txn.non_terminal().items():
            next_at = rec.get("next_retry_at")
            if next_at is None or next_at <= now:
                out.append(txn_id)
        return out

    def tick(self, now=None):
        """扫描一轮，返回本轮处理的事务ID。"""
        processed = []
        for txn_id in self.due_txns(now):
            processed.append(txn_id)
            self.deadletter.process(txn_id)
        return processed

    def run(self, stop_event=None, max_rounds=None):
        """常驻循环（供真实部署用；演示走 tick 由协调器驱动）。"""
        rounds = 0
        while True:
            if stop_event and stop_event.is_set():
                break
            if max_rounds and rounds >= max_rounds:
                break
            self.tick()
            rounds += 1
            time.sleep(self.interval)
