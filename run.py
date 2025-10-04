#!/usr/bin/env python3
"""
MySQL Workbench Python Edition
Main application entry point

Copyright (c) 2025 - Python rewrite of MySQL Workbench
Original MySQL Workbench: Copyright (c) 2007, 2025, Oracle and/or its affiliates

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0,
as published by the Free Software Foundation.
"""

import sys
import os
from pathlib import Path

# Add the package to the path
sys.path.insert(0, str(Path(__file__).parent))

from workbench.main import main

if __name__ == "__main__":
    main()
