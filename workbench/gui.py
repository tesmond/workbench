"""
Main application window and UI framework.
"""

import sys
import asyncio
from typing import Optional, Dict, Any
from pathlib import Path

from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QVBoxLayout,
    QHBoxLayout,
    QSplitter,
    QTabWidget,
    QStatusBar,
    QMenuBar,
    QToolBar,
    QWidget,
    QPushButton,
    QLabel,
    QMessageBox,
    QFileDialog,
    QDockWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QTextEdit,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QSpinBox,
    QCheckBox,
    QComboBox,
    QGroupBox,
    QProgressBar,
    QProgressDialog,
)
from PyQt6.QtCore import (
    Qt,
    QTimer,
    QThread,
    pyqtSignal,
    QSettings,
    QSize,
    QPoint,
    pyqtSlot,
)
from PyQt6.QtGui import (
    QAction,
    QIcon,
    QFont,
    QPixmap,
    QKeySequence,
    QShortcut,
    QCloseEvent,
    QColor,
)

from .config import settings, ConnectionProfile, DatabaseType
from .database import connection_manager, MySQLConnection, DatabaseObject
from .sql_editor import SQLEditor
from .database_browser import DatabaseBrowser


class ConnectionDialog(QDialog):
    """Dialog for creating/editing database connections"""

    def __init__(
        self, parent=None, connection_profile: Optional[ConnectionProfile] = None
    ):
        super().__init__(parent)
        self.connection_profile = connection_profile
        self.setup_ui()

        if connection_profile:
            self.load_connection_data()

    def setup_ui(self):
        """Setup the connection dialog UI"""
        self.setWindowTitle("Database Connection")
        self.setModal(True)
        self.setMinimumSize(400, 500)
        
        # Set dialog icon
        try:
            icon_path = Path(__file__).parent.parent.parent / "icon.png"
            if icon_path.exists():
                icon = QIcon(str(icon_path))
                self.setWindowIcon(icon)
        except Exception:
            pass  # Silently fail if icon can't be loaded

        layout = QVBoxLayout(self)

        # Basic connection info
        basic_group = QGroupBox("Connection Parameters")
        basic_layout = QFormLayout(basic_group)

        self.name_edit = QLineEdit()
        
        # Database type dropdown
        self.database_type_combo = QComboBox()
        self.database_type_combo.addItem("MySQL", DatabaseType.MYSQL)
        self.database_type_combo.addItem("PostgreSQL", DatabaseType.POSTGRESQL)
        self.database_type_combo.currentTextChanged.connect(self.on_database_type_changed)
        
        self.host_edit = QLineEdit()
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(3306)

        self.username_edit = QLineEdit()
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)

        self.default_schema_edit = QLineEdit()
        self.ssl_checkbox = QCheckBox("Use SSL")

        basic_layout.addRow("Connection Name:", self.name_edit)
        basic_layout.addRow("Database Type:", self.database_type_combo)
        basic_layout.addRow("Hostname:", self.host_edit)
        basic_layout.addRow("Port:", self.port_spin)
        basic_layout.addRow("Username:", self.username_edit)
        basic_layout.addRow("Password:", self.password_edit)
        basic_layout.addRow("Default Schema:", self.default_schema_edit)
        basic_layout.addRow("", self.ssl_checkbox)

        layout.addWidget(basic_group)

        # SSH tunnel settings
        ssh_group = QGroupBox("SSH Tunnel (Optional)")
        ssh_layout = QFormLayout(ssh_group)

        self.ssh_hostname_edit = QLineEdit()
        self.ssh_port_spin = QSpinBox()
        self.ssh_port_spin.setRange(1, 65535)
        self.ssh_port_spin.setValue(22)

        self.ssh_username_edit = QLineEdit()
        self.ssh_key_edit = QLineEdit()

        ssh_key_layout = QHBoxLayout()
        browse_key_btn = QPushButton("Browse...")
        browse_key_btn.clicked.connect(self.browse_ssh_key)
        ssh_key_layout.addWidget(self.ssh_key_edit)
        ssh_key_layout.addWidget(browse_key_btn)

        ssh_layout.addRow("SSH Hostname:", self.ssh_hostname_edit)
        ssh_layout.addRow("SSH Port:", self.ssh_port_spin)
        ssh_layout.addRow("SSH Username:", self.ssh_username_edit)
        ssh_layout.addRow("SSH Key File:", ssh_key_layout)

        layout.addWidget(ssh_group)

        # Test connection button
        test_btn = QPushButton("Test Connection")
        test_btn.clicked.connect(self.test_connection)
        layout.addWidget(test_btn)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def browse_ssh_key(self):
        """Browse for SSH key file"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select SSH Key File",
            str(Path.home()),
            "Key Files (*.pem *.key *.ppk);;All Files (*)",
        )
        if file_path:
            self.ssh_key_edit.setText(file_path)
    
    def on_database_type_changed(self, db_type_text: str):
        """Handle database type change to update default port"""
        if db_type_text == "MySQL":
            self.port_spin.setValue(3306)
        elif db_type_text == "PostgreSQL":
            self.port_spin.setValue(5432)

    def load_connection_data(self):
        """Load existing connection data into form"""
        if not self.connection_profile:
            return

        self.name_edit.setText(self.connection_profile.name)
        
        # Set database type
        db_type = getattr(self.connection_profile, 'database_type', DatabaseType.MYSQL)
        if db_type == DatabaseType.MYSQL:
            self.database_type_combo.setCurrentText("MySQL")
        else:
            self.database_type_combo.setCurrentText("PostgreSQL")
            
        self.host_edit.setText(self.connection_profile.host)
        self.port_spin.setValue(self.connection_profile.port)
        self.username_edit.setText(self.connection_profile.username)
        self.password_edit.setText(self.connection_profile.password)
        self.default_schema_edit.setText(self.connection_profile.default_schema)
        self.ssl_checkbox.setChecked(self.connection_profile.use_ssl)

        if self.connection_profile.ssh_hostname:
            self.ssh_hostname_edit.setText(self.connection_profile.ssh_hostname)
        if self.connection_profile.ssh_port:
            self.ssh_port_spin.setValue(self.connection_profile.ssh_port)
        if self.connection_profile.ssh_username:
            self.ssh_username_edit.setText(self.connection_profile.ssh_username)
        if self.connection_profile.ssh_key_file:
            self.ssh_key_edit.setText(str(self.connection_profile.ssh_key_file))

    def get_connection_profile(self) -> ConnectionProfile:
        """Get connection profile from form data"""
        # Get selected database type
        selected_db_type = self.database_type_combo.currentData()
        
        return ConnectionProfile(
            name=self.name_edit.text(),
            database_type=selected_db_type,
            host=self.host_edit.text(),
            port=self.port_spin.value(),
            username=self.username_edit.text(),
            password=self.password_edit.text(),
            default_schema=self.default_schema_edit.text(),
            use_ssl=self.ssl_checkbox.isChecked(),
            ssh_hostname=self.ssh_hostname_edit.text() or None,
            ssh_port=self.ssh_port_spin.value()
            if self.ssh_hostname_edit.text()
            else None,
            ssh_username=self.ssh_username_edit.text() or None,
            ssh_key_file=Path(self.ssh_key_edit.text())
            if self.ssh_key_edit.text()
            else None,
        )

    def test_connection(self):
        """Test the database connection"""
        profile = self.get_connection_profile()

        # Validate required fields
        if not profile.name or not profile.host or not profile.username:
            QMessageBox.warning(
                self,
                "Test Connection",
                "Please fill in required fields: Name, Host, and Username",
            )
            return

        # Show progress dialog
        progress = QProgressDialog("Testing connection...", "Cancel", 0, 0, self)
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.show()

        # Create a temporary connection for testing
        from .database import MySQLConnection

        test_conn = MySQLConnection(profile)

        try:
            # Test connection in main thread (blocking but with progress indicator)
            QApplication.processEvents()  # Allow UI to update

            success, message = test_conn.test_connection_sync()

            progress.close()

            if success:
                QMessageBox.information(self, "Test Connection", f"{message}")
            else:
                QMessageBox.critical(self, "Connection Failed", f"✗ {message}")

        except Exception as e:
            progress.close()
            QMessageBox.critical(
                self, "Connection Error", f"Unexpected error: {str(e)}"
            )


class DatabaseBrowser(QDockWidget):
    """Database object browser widget"""

    object_selected = pyqtSignal(DatabaseObject)

    def __init__(self, parent=None):
        super().__init__("Database Browser", parent)
        self.setObjectName("DatabaseBrowser")

        # Create tree widget
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Database Objects"])
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)

        self.setWidget(self.tree)

        # Connection references
        self.connections: Dict[str, MySQLConnection] = {}

    def add_connection(self, name: str, connection: MySQLConnection):
        """Add a connection to the browser"""
        self.connections[name] = connection

        # Add connection item to tree
        conn_item = QTreeWidgetItem(self.tree)
        conn_item.setText(0, name)
        conn_item.setData(
            0, Qt.ItemDataRole.UserRole, {"type": "connection", "name": name}
        )

        # Add loading placeholder
        loading_item = QTreeWidgetItem(conn_item)
        loading_item.setText(0, "Loading...")

    def refresh_connection(self, connection_name: str):
        """Refresh database objects for a connection"""
        # Find connection item
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if item.data(0, Qt.ItemDataRole.UserRole)["name"] == connection_name:
                self.load_databases(item, connection_name)
                break

    def load_databases(self, conn_item: QTreeWidgetItem, connection_name: str):
        """Load databases for a connection"""
        # Clear existing children
        conn_item.takeChildren()

        connection = self.connections.get(connection_name)
        if not connection or not connection.is_connected:
            error_item = QTreeWidgetItem(conn_item)
            error_item.setText(0, "Not connected")
            return

        # In a real implementation, this would be async
        # For now, just add some dummy items
        schemas = [
            "information_schema",
            "mysql",
            "performance_schema",
            "test_db",
            "sakila",
        ]
        for schema in schemas:
            if schema in ["information_schema", "mysql", "performance_schema"]:
                continue

            schema_item = QTreeWidgetItem(conn_item)
            schema_item.setText(0, schema)
            schema_item.setData(
                0,
                Qt.ItemDataRole.UserRole,
                {"type": "schema", "name": schema, "connection": connection_name},
            )

            # Add Tables folder
            tables_folder = QTreeWidgetItem(schema_item)
            tables_folder.setText(0, "Tables")
            tables_folder.setData(
                0,
                Qt.ItemDataRole.UserRole,
                {
                    "type": "folder",
                    "folder_type": "tables",
                    "schema": schema,
                    "connection": connection_name,
                },
            )

            # Add Views folder
            views_folder = QTreeWidgetItem(schema_item)
            views_folder.setText(0, "Views")
            views_folder.setData(
                0,
                Qt.ItemDataRole.UserRole,
                {
                    "type": "folder",
                    "folder_type": "views",
                    "schema": schema,
                    "connection": connection_name,
                },
            )

    def on_item_double_clicked(self, item: QTreeWidgetItem, column: int):
        """Handle item double-click"""
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if not data:
            return

        if data["type"] == "connection":
            # Expand/refresh connection
            self.refresh_connection(data["name"])
        elif data["type"] == "schema":
            # Expand schema
            item.setExpanded(not item.isExpanded())


class MainWindow(QMainWindow):
    """Main application window"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("MySQL Workbench - Python Edition (MySQL & PostgreSQL)")
        self.setObjectName("MySQLWorkbenchMainWindow")
        self.setMinimumSize(1000, 700)
        self.setup_window_icon()

        # Track current active connection
        self.current_connection = None

        # Load settings
        self.load_geometry()

        # Setup UI
        self.setup_ui()
        self.setup_menus()
        self.setup_toolbars()
        self.setup_status_bar()

        # Connect signals
        self.setup_connections()

    def setup_window_icon(self):
        """Setup window icon"""
        try:
            # Get path to icon.png in project root
            icon_path = Path(__file__) / "icon/icon.png"
            if icon_path.exists():
                icon = QIcon(str(icon_path))
                self.setWindowIcon(icon)
            else:
                # Fallback: try to get icon from application
                app = QApplication.instance()
                if app and hasattr(app, 'windowIcon'):
                    app_icon = app.windowIcon()
                    if not app_icon.isNull():
                        self.setWindowIcon(app_icon)
        except Exception as e:
            print(f"Failed to load window icon: {e}")

    def setup_ui(self):
        """Setup the main UI layout"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Main layout
        layout = QHBoxLayout(central_widget)

        # Main splitter
        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.main_splitter.setObjectName("MainSplitter")
        layout.addWidget(self.main_splitter)

        # Database browser (left panel) - import from new module
        from .database_browser import DatabaseBrowser as DatabaseBrowserWidget

        self.db_browser = DatabaseBrowserWidget(self)
        self.addDockWidget(Qt.DockWidgetArea.LeftDockWidgetArea, self.db_browser)

        # Central area with tabs
        self.tab_widget = QTabWidget()
        self.tab_widget.setObjectName("MainTabWidget")
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)

        # Welcome tab
        welcome_widget = QWidget()
        welcome_layout = QVBoxLayout(welcome_widget)
        
        welcome_label = QLabel("Welcome to MySQL Workbench - Python Edition")
        welcome_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        welcome_label.setStyleSheet("font-size: 16px; font-weight: bold; margin: 20px;")
        
        new_conn_btn = QPushButton("New Connection")
        new_conn_btn.clicked.connect(self.new_connection)
        new_conn_btn.setMaximumWidth(200)
        
        open_sql_btn = QPushButton("Open SQL Script")
        open_sql_btn.clicked.connect(self.open_sql_file)
        open_sql_btn.setMaximumWidth(200)
        
        welcome_layout.addStretch()
        welcome_layout.addWidget(welcome_label)
        welcome_layout.addWidget(new_conn_btn, alignment=Qt.AlignmentFlag.AlignCenter)
        welcome_layout.addWidget(open_sql_btn, alignment=Qt.AlignmentFlag.AlignCenter)
        welcome_layout.addStretch()
        
        self.tab_widget.addTab(welcome_widget, "Welcome")
        self.main_splitter.addWidget(self.tab_widget)

        # Set splitter proportions
        self.main_splitter.setSizes([250, 750])

    def setup_menus(self):
        """Setup application menus"""
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("File")

        new_connection_action = QAction("New Connection...", self)
        new_connection_action.setShortcut(QKeySequence.StandardKey.New)
        new_connection_action.triggered.connect(self.new_connection)
        file_menu.addAction(new_connection_action)

        file_menu.addSeparator()

        open_sql_action = QAction("Open SQL Script...", self)
        open_sql_action.setShortcut(QKeySequence.StandardKey.Open)
        open_sql_action.triggered.connect(self.open_sql_file)
        file_menu.addAction(open_sql_action)

        file_menu.addSeparator()

        exit_action = QAction("Exit", self)
        exit_action.setShortcut(QKeySequence.StandardKey.Quit)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Edit menu
        edit_menu = menubar.addMenu("Edit")

        # View menu
        view_menu = menubar.addMenu("View")
        view_menu.addAction(self.db_browser.toggleViewAction())

        # Query menu
        query_menu = menubar.addMenu("Query")

        new_query_action = QAction("New Query Tab", self)
        new_query_action.setShortcut(QKeySequence("Ctrl+T"))
        new_query_action.triggered.connect(self.new_query_tab)
        query_menu.addAction(new_query_action)

        # Help menu
        help_menu = menubar.addMenu("Help")

        about_action = QAction("About...", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)

    def setup_toolbars(self):
        """Setup toolbars"""
        main_toolbar = self.addToolBar("Main")
        main_toolbar.setObjectName("MainToolBar")

        new_conn_action = QAction("New Connection", self)
        new_conn_action.triggered.connect(self.new_connection)
        main_toolbar.addAction(new_conn_action)

        main_toolbar.addSeparator()

        new_query_action = QAction("New Query", self)
        new_query_action.triggered.connect(self.new_query_tab)
        main_toolbar.addAction(new_query_action)

    def setup_status_bar(self):
        """Setup status bar"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        # Main status label
        self.status_label = QLabel("Ready")
        self.status_bar.addWidget(self.status_label)

        # Connection status with icon
        connection_widget = QWidget()
        connection_layout = QHBoxLayout(connection_widget)
        connection_layout.setContentsMargins(0, 0, 0, 0)

        # Connection status indicator (colored circle)
        self.connection_indicator = QLabel("●")
        self.connection_indicator.setStyleSheet("color: red; font-size: 12px;")
        self.connection_indicator.setToolTip("Connection Status")

        self.connection_label = QLabel("No connections")

        connection_layout.addWidget(self.connection_indicator)
        connection_layout.addWidget(self.connection_label)

        self.status_bar.addPermanentWidget(connection_widget)

        # Server info label (hidden initially)
        self.server_info_label = QLabel("")
        self.server_info_label.hide()
        self.status_bar.addPermanentWidget(self.server_info_label)

    def setup_connections(self):
        """Setup signal connections"""
        # Connect database browser signals
        self.db_browser.object_double_clicked.connect(self.on_db_object_double_clicked)

        # Load saved connections on startup
        self.load_saved_connections()

    def load_saved_connections(self):
        """Load saved connection profiles and add them to the browser"""
        for connection_profile in settings.connections:
            # Create connection object (but don't connect yet)
            connection = connection_manager.add_connection(
                connection_profile.name, connection_profile
            )

            # Add to database browser
            self.db_browser.add_connection(connection_profile.name, connection)

    def on_query_executed(self, result):
        """Handle query execution result"""
        # Update status or handle result as needed
        pass

    def on_db_object_double_clicked(self, item):
        """Handle database object double-click"""
        # Generate appropriate query based on object type
        from .database_browser import DatabaseObjectType

        if hasattr(item, "object_type"):
            if item.object_type == DatabaseObjectType.TABLE:
                # Create SELECT query for table
                if item.schema_name and item.object_name and item.connection_name:
                    sql_editor = self.new_query_tab(item.connection_name)
                    query = f"SELECT * FROM `{item.schema_name}`.`{item.object_name}` LIMIT 1000;"
                    sql_editor.set_text(query)

    def new_connection(self):
        """Create a new database connection"""
        dialog = ConnectionDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            profile = dialog.get_connection_profile()
            
            # Save connection profile
            settings.add_connection(profile)
            
            # Create connection
            connection = connection_manager.add_connection(profile.name, profile)
            
            # Add to browser
            self.db_browser.add_connection(profile.name, connection)
            
            # Try to connect
            self.connect_to_database(profile.name)

    def connect_to_database(self, connection_name: str):
        """Connect to a database"""
        connection = connection_manager.get_connection(connection_name)
        if not connection:
            QMessageBox.warning(
                self, "Connection Error", f"Connection '{connection_name}' not found"
            )
            return

        # Show connecting status
        self.status_label.setText(f"Connecting to {connection_name}...")
        self.connection_label.setText("Connecting...")

        # Update connection indicator to yellow/orange (connecting)
        self.connection_indicator.setStyleSheet("color: orange; font-size: 12px;")
        self.connection_indicator.setToolTip(f"Connecting to {connection_name}")

        # Show progress dialog for connection
        progress = QProgressDialog(
            f"Connecting to {connection_name}...", "Cancel", 0, 0, self
        )
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.show()

        # Connect with error handling
        QTimer.singleShot(
            100, lambda: self._async_connect_helper(connection_name, progress)
        )

    def _async_connect_helper(self, connection_name: str, progress: QProgressDialog):
        """Helper to run async connection in proper event loop"""
        connection = connection_manager.get_connection(connection_name)
        if not connection:
            progress.close()
            return

        try:
            # Allow UI to update
            QApplication.processEvents()

            # Use synchronous connect method to avoid event loop issues
            success = connection.connect_sync()

            progress.close()

            if success:
                QTimer.singleShot(
                    100, lambda: self.on_connection_established(connection_name)
                )
            else:
                self.on_connection_failed(
                    connection_name, "Failed to establish connection"
                )

        except Exception as e:
            progress.close()
            self.on_connection_failed(connection_name, str(e))

    def on_connection_established(self, connection_name: str):
        """Handle successful database connection"""
        self.status_label.setText(f"Connected to {connection_name}")
        self.connection_label.setText(f"{connection_name}")

        # Track current connection
        self.current_connection = connection_name

        # Update connection indicator to green
        self.connection_indicator.setStyleSheet("color: green; font-size: 12px;")
        self.connection_indicator.setToolTip(f"Connected to {connection_name}")

        # Get server info and show it
        connection = connection_manager.get_connection(connection_name)
        if connection and connection.is_connected:
            try:
                # Get database type and connection info
                db_type = connection.profile.database_type.name
                if hasattr(connection.adapter, 'connection') and connection.adapter.connection:
                    if db_type == 'MYSQL':
                        server_info = connection.adapter.connection.get_server_info()
                        self.server_info_label.setText(f"MySQL {server_info}")
                    else:
                        # For PostgreSQL, show basic connection info
                        self.server_info_label.setText(f"PostgreSQL Connected")
                    self.server_info_label.show()
            except:
                pass

        # Show success message briefly
        QMessageBox.information(
            self,
            "Connection Successful",
            f"Successfully connected to {connection_name}",
        )

        # Refresh browser
        self.db_browser.refresh_connection(connection_name)

    def on_connection_failed(self, connection_name: str, error_message: str):
        """Handle failed database connection"""
        self.status_label.setText(f"Connection failed")
        self.connection_label.setText("Not connected")

        # Clear current connection
        self.current_connection = None

        # Update connection indicator to red
        self.connection_indicator.setStyleSheet("color: red; font-size: 12px;")
        self.connection_indicator.setToolTip("Not connected")

        # Hide server info
        self.server_info_label.hide()

        # Show error dialog
        QMessageBox.critical(
            self,
            "Connection Failed",
            f"Failed to connect to {connection_name}:\n\n{error_message}",
        )

    def new_query_tab(self, connection_name: Optional[str] = None):
        """Create a new SQL query tab"""
        # Use current connection if none specified
        if connection_name is None:
            connection_name = self.current_connection

        # Create SQL editor with connection
        sql_editor = SQLEditor(connection_name)

        # Connect signals
        sql_editor.query_executed.connect(self.on_query_executed)

        tab_name = f"Query {self.tab_widget.count()}"
        if connection_name:
            tab_name = f"Query - {connection_name}"
        else:
            tab_name = f"Query (No Connection)"

        tab_index = self.tab_widget.addTab(sql_editor, tab_name)
        self.tab_widget.setCurrentIndex(tab_index)

        return sql_editor

    def open_sql_file(self):
        """Open an SQL file"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Open SQL File", "", "SQL Files (*.sql);;All Files (*)"
        )

        if file_path:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # Create new tab with file content
                editor = QTextEdit()
                editor.setPlainText(content)
                editor.setFont(QFont("Consolas", 10))

                filename = Path(file_path).name
                tab_index = self.tab_widget.addTab(editor, filename)
                self.tab_widget.setCurrentIndex(tab_index)

            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to open file:\n{e}")

    def close_tab(self, index: int):
        """Close a tab"""
        if index > 0:  # Don't close welcome tab
            self.tab_widget.removeTab(index)

    def show_about(self):
        """Show about dialog"""
        QMessageBox.about(
            self,
            "About MySQL Workbench Python",
            "MySQL Workbench - Python Edition\n\n"
            "A modern Python implementation of the MySQL Workbench\n"
            "database administration and development tool.\n\n"
            "Version: 1.0.0\n"
            "License: GPL v2.0",
        )

    def load_geometry(self):
        """Load window geometry from settings"""
        qt_settings = QSettings("MySQL", "WorkbenchPython")
        geometry = qt_settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)

        state = qt_settings.value("windowState")
        if state:
            self.restoreState(state)

    def save_geometry(self):
        """Save window geometry to settings"""
        qt_settings = QSettings("MySQL", "WorkbenchPython")
        qt_settings.setValue("geometry", self.saveGeometry())
        qt_settings.setValue("windowState", self.saveState())



    def closeEvent(self, a0):
        """Handle application close"""
        if a0 is None:
            return

        self.save_geometry()

        # Disconnect all database connections synchronously
        try:
            # Simple synchronous cleanup - use adapter disconnect methods
            for connection in connection_manager.connections.values():
                try:
                    connection.disconnect_sync()
                except Exception as conn_error:
                    print(f"Error disconnecting {connection.profile.name}: {conn_error}")
        except Exception as e:
            print(f"Error during cleanup: {e}")

        a0.accept()
