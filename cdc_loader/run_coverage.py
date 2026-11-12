#!/usr/bin/env python3
"""采样覆盖审计层隔离模式启动器（与 run_cli.py / run_reading.py 同一约定）。

用法：
    python3 -I run_coverage.py <子命令> [参数...]
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from coverage.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
