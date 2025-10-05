#!/usr/bin/env python3
"""
SQL Workbench
Main application entry point


This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License, version 2.0,
as published by the Free Software Foundation.
"""

import sys
from pathlib import Path

# Add the package to the path
sys.path.insert(0, str(Path(__file__).parent))

from workbench.main import main

if __name__ == "__main__":
    main()
