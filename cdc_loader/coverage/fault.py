"""覆盖审计的确定性故障注入点。

``after_detail_before_confirm``
    审计明细（audit_expected / audit_matches / audit_gaps /
    audit_unmatched）已在事务内写完、但 audit_runs 还没翻成 published
    之前。注入方式是 ``os._exit(42)``：跳过所有 Python 层清理与缓冲
    刷新，模拟“明细已经写入、对外确认之前真实退出进程”。

明细写入与发布位於同一条 SQLite 事务，崩溃后未提交事务整体回滚，只
留下阶段一已提交的 building 残档；重开后 published 审计不受影响，
building 残档明确拒绝对外读取（AuditStale），用同一口径重做即安全
清理并发布。每个注入点在一次进程生命周期内只触发一次。
"""

from __future__ import annotations

import os
import sys

FAULT_AFTER_DETAIL_BEFORE_CONFIRM = "after_detail_before_confirm"

ALL_FAULTS = frozenset({FAULT_AFTER_DETAIL_BEFORE_CONFIRM})

CRASH_EXIT_CODE = 42


class FaultInjector:
    def __init__(self, fault: str | None = None) -> None:
        if fault is not None and fault not in ALL_FAULTS:
            raise ValueError(f"未知故障注入点: {fault}，允许: {sorted(ALL_FAULTS)}")
        self._fault = fault
        self._fired = False

    @property
    def fault(self) -> str | None:
        return self._fault

    def hit(self, point: str) -> None:
        if self._fired or point != self._fault:
            return
        self._fired = True
        sys.stderr.write(
            f"[fault-inject] 命中注入点 {point!r}，进程以退出码 {CRASH_EXIT_CODE} 终止\n"
        )
        sys.stderr.flush()
        os._exit(CRASH_EXIT_CODE)
