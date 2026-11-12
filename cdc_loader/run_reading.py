#!/usr/bin/env python3
"""读数事实仓隔离模式启动器（与 run_cli.py / run_entity.py 同一约定）。

用法：
    python3 -I run_reading.py <子命令> [参数...]
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from reading.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
