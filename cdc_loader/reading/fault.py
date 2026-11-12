"""统计构建的确定性故障注入点。

``after_detail_before_publish``
    新口径的统计明细（stats_observations / stats_attribution /
    stats_cells）已在事务内写完、但 stats_runs 还没翻成 published
    之前。注入方式是 ``os._exit(42)``：跳过所有 Python 层清理与缓冲
    刷新，模拟真实进程崩溃。

由于明细写入与“发布”位於同一条 SQLite 事务，崩溃后未提交事务整体
回滚，在线日报仍指向上一份 published 结果；重开后该口径只剩/或不留
building 残档，用同一对口径重建即可。每个注入点在一次进程生命周期内
只触发一次。生产环境不传 fault 参数即为空操作。
"""

from __future__ import annotations

import os
import sys

FAULT_AFTER_DETAIL_BEFORE_PUBLISH = "after_detail_before_publish"

ALL_FAULTS = frozenset({FAULT_AFTER_DETAIL_BEFORE_PUBLISH})

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
