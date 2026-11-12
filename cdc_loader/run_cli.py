#!/usr/bin/env python3
"""隔离模式启动器。

``python3 -I`` 不会把当前目录或脚本目录加入 ``sys.path``，因此无法直接
``-m cdc.cli``。本启动器显式把项目根加入路径后转交 :mod:`cdc.cli`，
使数据处理可以全程运行在隔离模式下（``-I`` 同时忽略 PYTHONPATH 与用户
site-packages，数据目录也不会出现在导入路径中）。

用法：
    python3 -I run_cli.py <子命令> [参数...]
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from cdc.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
