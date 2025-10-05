#!/usr/bin/env python3
"""
Launch script for Workbench with error checking.
"""

import subprocess
import sys
from pathlib import Path


def check_syntax():
    """Check Python syntax before launching"""
    print("Checking syntax...")
    try:
        result = subprocess.run(
            [sys.executable, "validate_syntax.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent,
        )

        if result.returncode == 0:
            print("Syntax validation passed")
            return True
        else:
            print("✗ Syntax errors found:")
            print(result.stdout)
            return False

    except Exception as e:
        print(f"Could not run syntax validation: {e}")
        return True  # Continue anyway


def launch_application():
    """Launch the main application"""
    print("Launching SQL Workbench...")
    try:
        # Import and run the application
        sys.path.insert(0, str(Path(__file__).parent))
        from workbench.main import main

        return main()

    except Exception as e:
        print(f"Failed to launch application: {e}")
        print("\nTroubleshooting:")
        print(
            "1. Make sure all dependencies are installed: pip install -r requirements.txt"
        )
        print("2. Check that you're in the correct directory")
        print("3. Verify Python version is 3.9+")
        return 1


def main():
    """Main launcher function"""
    print("SQL Workbench - Launcher")
    print("=" * 45)

    # Check syntax first
    if not check_syntax():
        print("\nPlease fix syntax errors before launching.")
        return 1

    # Launch application
    return launch_application()


if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\nLaunch interrupted.")
        exit(1)
    except Exception as e:
        print(f"Launch error: {e}")
        exit(1)
