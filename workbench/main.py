"""
Main application entry point for MySQL Workbench Python Edition.
"""

import sys
import os
import logging
import asyncio
from pathlib import Path
from typing import Optional

from PyQt6.QtWidgets import QApplication, QMessageBox, QSplashScreen
from PyQt6.QtCore import Qt, QTimer, qInstallMessageHandler, QtMsgType
from PyQt6.QtGui import QPixmap, QFont, QPalette, QColor, QIcon

from .config import settings
from .gui import MainWindow
from .sql_editor import SQLEditor
from .database_browser import DatabaseBrowser


def setup_logging():
    """Setup application logging"""
    log_dir = settings.config_dir / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / "workbench.log"

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    # Setup Qt message handler
    def qt_message_handler(mode: QtMsgType, context, message: str):
        if mode == QtMsgType.QtDebugMsg:
            logging.debug(f"Qt: {message}")
        elif mode == QtMsgType.QtWarningMsg:
            logging.warning(f"Qt: {message}")
        elif mode == QtMsgType.QtCriticalMsg:
            logging.error(f"Qt: {message}")
        elif mode == QtMsgType.QtFatalMsg:
            logging.critical(f"Qt: {message}")

    qInstallMessageHandler(qt_message_handler)


def setup_application_style(app: QApplication):
    """Setup application appearance and styling"""
    # Set application properties
    app.setApplicationName("MySQL Workbench Python")
    app.setApplicationVersion("1.0.0")
    app.setOrganizationName("MySQL Workbench Python Team")
    app.setOrganizationDomain("mysql-workbench-python.org")

    # Set default font
    font = QFont(settings.editor.font_family, settings.editor.font_size)
    app.setFont(font)

    # Apply theme if specified
    if settings.ui.theme == "dark":
        apply_dark_theme(app)
    elif settings.ui.theme == "light":
        apply_light_theme(app)
    # "system" theme uses default system colors


def apply_dark_theme(app: QApplication):
    """Apply dark theme to application"""
    palette = QPalette()

    # Window colors
    palette.setColor(QPalette.ColorRole.Window, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.WindowText, QColor(255, 255, 255))

    # Base colors (input fields)
    palette.setColor(QPalette.ColorRole.Base, QColor(25, 25, 25))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor(53, 53, 53))

    # Text colors
    palette.setColor(QPalette.ColorRole.Text, QColor(255, 255, 255))

    # Button colors
    palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
    palette.setColor(QPalette.ColorRole.ButtonText, QColor(255, 255, 255))

    # Highlight colors
    palette.setColor(QPalette.ColorRole.Highlight, QColor(42, 130, 218))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor(0, 0, 0))

    app.setPalette(palette)


def apply_light_theme(app: QApplication):
    """Apply light theme to application"""
    # Light theme uses mostly default colors with some tweaks
    palette = QPalette()

    # Slightly off-white background
    palette.setColor(QPalette.ColorRole.Window, QColor(248, 248, 248))
    palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))

    app.setPalette(palette)


class Application(QApplication):
    """Main application class"""

    def __init__(self, argv):
        super().__init__(argv)

        self.main_window: Optional[MainWindow] = None
        self.splash: Optional[QSplashScreen] = None

        # Setup application
        setup_logging()
        setup_application_style(self)
        self.setup_application_icon()

        # Install exception handler
        sys.excepthook = self.exception_handler

    def setup_application_icon(self):
        """Setup application icon"""
        try:
            # Get path to icon.png in project root
            icon_path = Path(__file__).parent / "icons/icon.png"
            if icon_path.exists():
                icon = QIcon(str(icon_path))
                self.setWindowIcon(icon)
                logging.info(f"Application icon loaded from: {icon_path}")
            else:
                logging.warning(f"Icon file not found at: {icon_path}")
        except Exception as e:
            logging.error(f"Failed to load application icon: {e}")

    def show_splash_screen(self):
        """Show application splash screen"""
        # Create a simple splash screen
        splash_pixmap = QPixmap(400, 200)
        splash_pixmap.fill(QColor(42, 130, 218))

        self.splash = QSplashScreen(splash_pixmap)
        self.splash.setWindowFlags(
            Qt.WindowType.WindowStaysOnTopHint | Qt.WindowType.SplashScreen
        )

        # Add text to splash screen
        self.splash.showMessage(
            """
MySQL Workbench - Python Edition
Version 1.0.0

Loading...
            """,
            Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignBottom,
            QColor(255, 255, 255),
        )

        self.splash.show()
        self.processEvents()

    def hide_splash_screen(self):
        """Hide splash screen"""
        if self.splash:
            self.splash.close()
            self.splash = None

    def initialize_main_window(self):
        """Initialize and show main window"""
        try:
            self.main_window = MainWindow()

            # Connect application signals
            self.aboutToQuit.connect(self.on_about_to_quit)

            # Show main window
            self.main_window.show()

            # Hide splash screen
            QTimer.singleShot(1000, self.hide_splash_screen)

            logging.info("MySQL Workbench Python Edition started successfully")

        except Exception as e:
            logging.critical(f"Failed to initialize main window: {e}")
            self.show_error_message(
                "Initialization Error", f"Failed to start application:\\n{e}"
            )
            return False

        return True

    def on_about_to_quit(self):
        """Handle application quit"""
        logging.info("Application shutting down...")

        # Save settings
        if self.main_window:
            self.main_window.save_geometry()

        # Cleanup
        # In a real implementation, ensure all connections are closed

    def exception_handler(self, exc_type, exc_value, exc_traceback):
        """Global exception handler"""
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        import traceback

        error_msg = "".join(
            traceback.format_exception(exc_type, exc_value, exc_traceback)
        )

        logging.critical(f"Uncaught exception: {error_msg}")

        # Show error dialog
        self.show_error_message(
            "Application Error", f"An unexpected error occurred:\\n\\n{exc_value}"
        )

    def show_error_message(self, title: str, message: str):
        """Show error message dialog"""
        msg_box = QMessageBox()
        msg_box.setIcon(QMessageBox.Icon.Critical)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg_box.exec()


def check_dependencies():
    """Check if all required dependencies are available"""
    missing_deps = []

    try:
        import PyQt6
    except ImportError:
        missing_deps.append("PyQt6")

    try:
        import pymysql
    except ImportError:
        missing_deps.append("PyMySQL")

    try:
        import paramiko
    except ImportError:
        missing_deps.append("paramiko")

    try:
        from pygments import highlight
    except ImportError:
        missing_deps.append("pygments")

    if missing_deps:
        print(f"Missing required dependencies: {', '.join(missing_deps)}")
        print("Please install them using:")
        print(f"pip install {' '.join(missing_deps)}")
        return False

    return True


def main():
    """Main application entry point"""
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)

    # Set up asyncio policy for Windows
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # Create application
    app = Application(sys.argv)

    # Show splash screen
    app.show_splash_screen()

    # Initialize main window
    if not app.initialize_main_window():
        sys.exit(1)

    # Start event loop
    try:
        sys.exit(app.exec())
    except KeyboardInterrupt:
        logging.info("Application interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
