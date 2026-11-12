"""实体归并层的确定性故障注入点。

``after_ledger_before_decision``
    本批修订行与批次登记已经写入（同一 SQLite 事务内）、但决策尚未
    计算更未提交之前。这是最危险的位置：台账写入与决策落定之间。

注入方式是 ``os._exit(42)``：跳过所有 Python 层清理与缓冲刷新，
模拟真实进程崩溃。由于修订写入与决策落定位於**同一条 SQLite 事务**，
崩溃后未提交事务整体回滚，重开台账看不到半批状态；用完全相同的
批次文件重投即可。每个注入点在一次进程生命周期内只触发一次。

生产环境不传 fault 参数即可，注入器永远为空操作。
"""

from __future__ import annotations

import os
import sys

FAULT_AFTER_LEDGER_BEFORE_DECISION = "after_ledger_before_decision"

ALL_FAULTS = frozenset({FAULT_AFTER_LEDGER_BEFORE_DECISION})

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
        """到达注入点；若命中则以确定性方式终止本进程。"""
        if self._fired or point != self._fault:
            return
        self._fired = True
        sys.stderr.write(
            f"[fault-inject] 命中注入点 {point!r}，进程以退出码 {CRASH_EXIT_CODE} 终止\n"
        )
        sys.stderr.flush()
        # os._exit 不执行 finally / atexit / 缓冲刷新，最接近真实崩溃。
        os._exit(CRASH_EXIT_CODE)
