"""
Enhanced database browser with full MySQL object support.
"""

import asyncio
import logging
from typing import Optional, List, Dict, Any
from enum import Enum

from PyQt6.QtWidgets import (
    QDockWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QHBoxLayout,
    QWidget,
    QPushButton,
    QLineEdit,
    QMenu,
    QMessageBox,
    QDialog,
    QApplication,
    QProgressBar,
    QLabel,
)
from PyQt6.QtCore import (
    Qt,
    pyqtSignal,
    QThread,
    QTimer,
    QObject,
    QRunnable,
    QThreadPool,
    pyqtSlot,
)
from PyQt6.QtGui import QIcon, QAction, QPixmap

from .database import connection_manager, MySQLConnection, DatabaseObject


logger = logging.getLogger(__name__)


class DatabaseObjectType(Enum):
    """Database object types"""

    CONNECTION = "connection"
    SCHEMA = "schema"
    TABLE = "table"
    VIEW = "view"
    PROCEDURE = "procedure"
    FUNCTION = "function"
    TRIGGER = "trigger"
    COLUMN = "column"
    INDEX = "index"
    FOREIGN_KEY = "foreign_key"
    FOLDER = "folder"


class DatabaseTreeItem(QTreeWidgetItem):
    """Enhanced tree item for database objects"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.object_type: Optional[DatabaseObjectType] = None
        self.connection_name: Optional[str] = None
        self.schema_name: Optional[str] = None
        self.object_name: Optional[str] = None
        self.parent_object: Optional[str] = None
        self.loaded = False
        self.extra_data: Dict[str, Any] = {}

    def set_database_object(self, obj: DatabaseObject, connection_name: str):
        """Set database object information"""
        self.object_name = obj.name
        self.connection_name = connection_name
        self.schema_name = obj.schema
        self.parent_object = obj.parent

        # Map object type
        type_mapping = {
            "schema": DatabaseObjectType.SCHEMA,
            "table": DatabaseObjectType.TABLE,
            "view": DatabaseObjectType.VIEW,
            "procedure": DatabaseObjectType.PROCEDURE,
            "function": DatabaseObjectType.FUNCTION,
            "trigger": DatabaseObjectType.TRIGGER,
            "column": DatabaseObjectType.COLUMN,
            "index": DatabaseObjectType.INDEX,
            "foreign_key": DatabaseObjectType.FOREIGN_KEY,
        }

        self.object_type = type_mapping.get(obj.object_type, DatabaseObjectType.TABLE)
        self.extra_data = obj.extra_info or {}

        # Set display text
        self.setText(0, obj.name)

        # Set icon based on type
        self.update_icon()

    def set_folder(
        self,
        folder_name: str,
        folder_type: str,
        connection_name: str,
        schema_name: str | None = None,
    ):
        """Set as folder item"""
        self.object_type = DatabaseObjectType.FOLDER
        self.connection_name = connection_name
        self.schema_name = schema_name
        self.object_name = folder_name
        self.extra_data = {"folder_type": folder_type}

        self.setText(0, folder_name)
        self.update_icon()

    def set_connection(self, connection_name: str):
        """Set as connection item"""
        self.object_type = DatabaseObjectType.CONNECTION
        self.connection_name = connection_name
        self.object_name = connection_name

        self.setText(0, connection_name)
        self.update_icon()

    def update_icon(self):
        """Update item icon based on type"""
        # In a real implementation, you would load actual icons
        # For now, we'll use text indicators
        icon_map = {
            DatabaseObjectType.CONNECTION: "🔗",
            DatabaseObjectType.SCHEMA: "🗃️",
            DatabaseObjectType.TABLE: "📋",
            DatabaseObjectType.VIEW: "👁️",
            DatabaseObjectType.PROCEDURE: "⚙️",
            DatabaseObjectType.FUNCTION: "🔧",
            DatabaseObjectType.TRIGGER: "⚡",
            DatabaseObjectType.COLUMN: "📝",
            DatabaseObjectType.INDEX: "🔍",
            DatabaseObjectType.FOREIGN_KEY: "🔗",
            DatabaseObjectType.FOLDER: "📁",
        }

        icon_text = icon_map.get(self.object_type, "❓")
        current_text = self.text(0)
        if not current_text.startswith(icon_text):
            self.setText(0, f"{icon_text} {current_text}")

    def needs_loading(self) -> bool:
        """Check if this item needs to load children"""
        return not self.loaded and (
            self.object_type
            in [
                DatabaseObjectType.CONNECTION,
                DatabaseObjectType.SCHEMA,
                DatabaseObjectType.FOLDER,
                DatabaseObjectType.TABLE,
            ]
        )


class DatabaseBrowserWorker(QRunnable):
    """Worker for loading database objects in background"""

    def __init__(self, connection_name: str, operation: str, **kwargs):
        super().__init__()
        self.connection_name = connection_name
        self.operation = operation
        self.kwargs = kwargs
        self.signals = DatabaseBrowserSignals()

    @pyqtSlot()
    def run(self):
        """Execute the database operation"""
        try:
            connection = connection_manager.get_connection(self.connection_name)
            if not connection or not connection.is_connected:
                self.signals.error.emit("Connection not available")
                return

            if self.operation == "load_schemas":
                # Use actual database connection to get real schemas
                try:
                    # Run async method in this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    schemas = loop.run_until_complete(connection.get_databases())
                    loop.close()

                    self.signals.schemas_loaded.emit(schemas)

                except Exception as e:
                    self.signals.error.emit(f"Failed to load schemas: {str(e)}")
            
            elif self.operation == "load_databases":
                # Load databases for PostgreSQL
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    databases = loop.run_until_complete(connection.get_databases())
                    loop.close()

                    self.signals.databases_loaded.emit(databases)

                except Exception as e:
                    self.signals.error.emit(f"Failed to load databases: {str(e)}")
            
            elif self.operation == "load_database_schemas":
                # Load schemas within a PostgreSQL database
                database = self.kwargs.get("database")
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    schemas = loop.run_until_complete(connection.get_schemas(database))
                    loop.close()

                    self.signals.database_schemas_loaded.emit(database, schemas)

                except Exception as e:
                    self.signals.error.emit(f"Failed to load schemas for database {database}: {str(e)}")

            elif self.operation == "load_tables":
                schema = self.kwargs.get("schema")
                try:
                    logger.info(f"Worker loading tables for schema: {schema}")
                    # Use actual database connection to get real tables
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    tables = loop.run_until_complete(connection.get_tables(schema))
                    loop.close()
                    
                    logger.info(f"Worker loaded {len(tables)} tables for schema: {schema}")
                    self.signals.tables_loaded.emit(schema, tables)

                except Exception as e:
                    logger.error(f"Worker failed to load tables for {schema}: {str(e)}")
                    self.signals.error.emit(
                        f"Failed to load tables for {schema}: {str(e)}"
                    )

            elif self.operation == "load_columns":
                schema = self.kwargs.get("schema")
                table = self.kwargs.get("table")
                try:
                    # Use actual database connection to get real columns
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    columns = loop.run_until_complete(
                        connection.get_table_columns(schema, table)
                    )
                    loop.close()

                    self.signals.columns_loaded.emit(schema, table, columns)

                except Exception as e:
                    self.signals.error.emit(
                        f"Failed to load columns for {schema}.{table}: {str(e)}"
                    )

        except Exception as e:
            self.signals.error.emit(str(e))


class DatabaseBrowserSignals(QObject):
    """Signals for database browser worker"""

    schemas_loaded = pyqtSignal(list)
    databases_loaded = pyqtSignal(list)
    database_schemas_loaded = pyqtSignal(str, list)  # database, schemas
    tables_loaded = pyqtSignal(str, list)  # schema, tables
    columns_loaded = pyqtSignal(str, str, list)  # schema, table, columns
    error = pyqtSignal(str)


class DatabaseBrowser(QDockWidget):
    """Enhanced database browser with full object support"""

    object_selected = pyqtSignal(DatabaseTreeItem)
    object_double_clicked = pyqtSignal(DatabaseTreeItem)

    def __init__(self, parent=None):
        super().__init__("Database Browser", parent)
        self.setObjectName("DatabaseBrowser")

        self.connections: Dict[str, MySQLConnection] = {}
        self.thread_pool = QThreadPool()

        self.setup_ui()

    def setup_ui(self):
        """Setup the browser UI"""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Filter/search box
        search_layout = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter objects...")
        self.search_edit.textChanged.connect(self.filter_objects)

        refresh_btn = QPushButton("🔄")
        refresh_btn.setMaximumWidth(30)
        refresh_btn.clicked.connect(self.refresh_all)
        refresh_btn.setToolTip("Refresh all connections")

        search_layout.addWidget(self.search_edit)
        search_layout.addWidget(refresh_btn)
        layout.addLayout(search_layout)

        # Tree widget
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Database Objects"])
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

        # Connect signals
        self.tree.itemExpanded.connect(self.on_item_expanded)
        self.tree.itemClicked.connect(self.on_item_clicked)
        self.tree.itemDoubleClicked.connect(self.on_item_double_clicked)
        self.tree.customContextMenuRequested.connect(self.show_context_menu)

        layout.addWidget(self.tree)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        self.setWidget(widget)

    def add_connection(self, name: str, connection: MySQLConnection):
        """Add a database connection to the browser"""
        self.connections[name] = connection

        # Create connection item
        conn_item = DatabaseTreeItem(self.tree)
        conn_item.set_connection(name)

        # Add loading placeholder
        if connection.is_connected:
            self.load_connection_objects(conn_item)
        else:
            loading_item = DatabaseTreeItem(conn_item)
            loading_item.setText(0, "Not connected")

    def remove_connection(self, name: str):
        """Remove a connection from the browser"""
        if name in self.connections:
            del self.connections[name]

        # Remove from tree
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if isinstance(item, DatabaseTreeItem) and item.connection_name == name:
                self.tree.takeTopLevelItem(i)
                break

    def refresh_connection(self, connection_name: str):
        """Refresh a specific connection"""
        connection = self.connections.get(connection_name)
        if not connection:
            return

        # Find connection item
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if (
                isinstance(item, DatabaseTreeItem)
                and item.connection_name == connection_name
            ):
                # Clear children
                item.takeChildren()
                item.loaded = False

                if connection.is_connected:
                    self.load_connection_objects(item)
                else:
                    not_conn_item = DatabaseTreeItem(item)
                    not_conn_item.setText(0, "Not connected")
                break

    def refresh_all(self):
        """Refresh all connections"""
        for conn_name in self.connections.keys():
            self.refresh_connection(conn_name)

    def refresh_connections(self):
        """Refresh the connection list (rebuild from settings)"""
        # Clear existing items
        self.tree.clear()
        self.connections.clear()

        # Reload from settings
        from .config import settings

        for connection_profile in settings.connections:
            # Create connection object (but don't connect yet)
            connection = connection_manager.add_connection(
                connection_profile.name, connection_profile
            )

            # Add to database browser
            self.add_connection(connection_profile.name, connection)

    def load_connection_objects(self, conn_item: DatabaseTreeItem):
        """Load objects for a connection"""
        if not conn_item.connection_name:
            return

        # Show loading
        conn_item.takeChildren()
        loading_item = DatabaseTreeItem(conn_item)
        loading_item.setText(0, "Loading...")

        # Check database type to determine loading strategy
        connection = connection_manager.get_connection(conn_item.connection_name)
        if connection and hasattr(connection, 'profile') and hasattr(connection.profile, 'database_type'):
            from .config import DatabaseType
            if connection.profile.database_type == DatabaseType.POSTGRESQL:
                # For PostgreSQL: Load databases first
                worker = DatabaseBrowserWorker(conn_item.connection_name, "load_databases")
                worker.signals.databases_loaded.connect(
                    lambda databases: self.on_databases_loaded(conn_item, databases)
                )
                worker.signals.error.connect(lambda error: self.on_load_error(conn_item, error))
            else:
                # For MySQL: Load schemas directly
                worker = DatabaseBrowserWorker(conn_item.connection_name, "load_schemas")
                worker.signals.schemas_loaded.connect(
                    lambda schemas: self.on_schemas_loaded(conn_item, schemas)
                )
                worker.signals.error.connect(lambda error: self.on_load_error(conn_item, error))
        else:
            # Default behavior for MySQL
            worker = DatabaseBrowserWorker(conn_item.connection_name, "load_schemas")
            worker.signals.schemas_loaded.connect(
                lambda schemas: self.on_schemas_loaded(conn_item, schemas)
            )
            worker.signals.error.connect(lambda error: self.on_load_error(conn_item, error))

        self.thread_pool.start(worker)

    def on_schemas_loaded(
        self, conn_item: DatabaseTreeItem, schemas: List[DatabaseObject]
    ):
        """Handle schemas loaded"""
        conn_item.takeChildren()

        for schema in schemas:
            schema_item = DatabaseTreeItem(conn_item)
            schema_item.set_database_object(schema, conn_item.connection_name)

            # Add folder structure
            tables_folder = DatabaseTreeItem(schema_item)
            tables_folder.set_folder(
                "Tables", "tables", conn_item.connection_name, schema.name
            )

            views_folder = DatabaseTreeItem(schema_item)
            views_folder.set_folder(
                "Views", "views", conn_item.connection_name, schema.name
            )

            procedures_folder = DatabaseTreeItem(schema_item)
            procedures_folder.set_folder(
                "Stored Procedures",
                "procedures",
                conn_item.connection_name,
                schema.name,
            )

            functions_folder = DatabaseTreeItem(schema_item)
            functions_folder.set_folder(
                "Functions", "functions", conn_item.connection_name, schema.name
            )

        conn_item.loaded = True
        conn_item.setExpanded(True)

    def on_databases_loaded(self, conn_item: DatabaseTreeItem, databases: List[DatabaseObject]):
        """Handle databases loaded for PostgreSQL"""
        conn_item.takeChildren()

        for database in databases:
            db_item = DatabaseTreeItem(conn_item)
            # For PostgreSQL databases, we need to override the object type since they're returned as 'database'
            # but the UI expects them to behave like expandable containers
            db_item.object_type = DatabaseObjectType.SCHEMA  # Reuse schema type for expandability
            db_item.connection_name = conn_item.connection_name
            db_item.object_name = database.name
            db_item.setText(0, database.name)
            db_item.update_icon()
            # Mark that this database needs to load schemas
            db_item.loaded = False

        conn_item.loaded = True
        conn_item.setExpanded(True)

    def load_database_schemas(self, db_item: DatabaseTreeItem):
        """Load schemas for a PostgreSQL database"""
        if not db_item.connection_name or not db_item.object_name:
            return

        # Show loading
        db_item.takeChildren()
        loading_item = DatabaseTreeItem(db_item)
        loading_item.setText(0, "Loading schemas...")

        # Load schemas for this database
        worker = DatabaseBrowserWorker(db_item.connection_name, "load_database_schemas", database=db_item.object_name)
        worker.signals.database_schemas_loaded.connect(
            lambda database, schemas: self.on_database_schemas_loaded(db_item, database, schemas)
        )
        worker.signals.error.connect(lambda error: self.on_load_error(db_item, error))

        self.thread_pool.start(worker)

    def on_database_schemas_loaded(self, db_item: DatabaseTreeItem, database: str, schemas: List[DatabaseObject]):
        """Handle schemas loaded for a PostgreSQL database"""
        db_item.takeChildren()

        for schema in schemas:
            schema_item = DatabaseTreeItem(db_item)
            schema_item.set_database_object(schema, db_item.connection_name)

            # Add folder structure for tables within schema
            # For PostgreSQL, store database.schema format to provide context
            schema_context = f"{database}.{schema.name}"
            tables_folder = DatabaseTreeItem(schema_item)
            tables_folder.set_folder(
                "Tables", "tables", db_item.connection_name, schema_context
            )

            views_folder = DatabaseTreeItem(schema_item)
            views_folder.set_folder(
                "Views", "views", db_item.connection_name, schema.name
            )

            procedures_folder = DatabaseTreeItem(schema_item)
            procedures_folder.set_folder(
                "Stored Procedures",
                "procedures",
                db_item.connection_name,
                schema.name,
            )

            functions_folder = DatabaseTreeItem(schema_item)
            functions_folder.set_folder(
                "Functions", "functions", db_item.connection_name, schema.name
            )

        db_item.loaded = True
        db_item.setExpanded(True)

    def on_tables_loaded(self, schema: str, tables: List[DatabaseObject]):
        """Handle tables loaded for a schema"""
        # Find the tables folder
        tables_folder = self.find_folder_item("tables", schema)
        if not tables_folder:
            return

        tables_folder.takeChildren()

        for table in tables:
            table_item = DatabaseTreeItem(tables_folder)
            table_item.set_database_object(table, tables_folder.connection_name)

            # Add columns placeholder
            columns_item = DatabaseTreeItem(table_item)
            columns_item.setText(0, "Columns")

        tables_folder.loaded = True

    def on_columns_loaded(self, schema: str, table: str, columns: List[DatabaseObject]):
        """Handle columns loaded for a table"""
        # Find the table item
        table_item = self.find_table_item(schema, table)
        if not table_item:
            return

        table_item.takeChildren()

        for column in columns:
            col_item = DatabaseTreeItem(table_item)
            col_item.set_database_object(column, table_item.connection_name)

            # Add type info to display
            if column.extra_info:
                data_type = column.extra_info.get("data_type", "")
                nullable = "NULL" if column.extra_info.get("nullable") else "NOT NULL"
                key = column.extra_info.get("key", "")

                detail_text = f"{column.name} ({data_type}) {nullable}"
                if key:
                    detail_text += f" [{key}]"
                col_item.setText(0, f"📝 {detail_text}")

        table_item.loaded = True

    def on_load_error(self, item: DatabaseTreeItem, error: str):
        """Handle loading error"""
        # Log the full error for debugging
        logger.error(f"Database browser load error for item '{item.text(0) if item else 'unknown'}': {error}")
        
        if item:
            item.takeChildren()
            error_item = DatabaseTreeItem(item)
            error_item.setText(0, f"❌ Error: {error}")
        
        # Also show a message box for critical errors
        from PyQt6.QtWidgets import QMessageBox
        if "Failed to load schemas" in error or "Failed to load databases" in error:
            QMessageBox.warning(
                self.parent() if hasattr(self, 'parent') else None,
                "Database Loading Error", 
                f"Failed to load database objects:\n\n{error}\n\nCheck the connection settings and server availability."
            )

    def find_folder_item(
        self, folder_type: str, schema: str
    ) -> Optional[DatabaseTreeItem]:
        """Find a folder item by type and schema"""
        for i in range(self.tree.topLevelItemCount()):
            conn_item = self.tree.topLevelItem(i)
            for j in range(conn_item.childCount()):
                schema_item = conn_item.child(j)
                if (
                    isinstance(schema_item, DatabaseTreeItem)
                    and schema_item.object_name == schema
                ):
                    for k in range(schema_item.childCount()):
                        folder_item = schema_item.child(k)
                        if (
                            isinstance(folder_item, DatabaseTreeItem)
                            and folder_item.object_type == DatabaseObjectType.FOLDER
                            and folder_item.extra_data.get("folder_type") == folder_type
                        ):
                            return folder_item
        return None

    def find_table_item(self, schema: str, table: str) -> Optional[DatabaseTreeItem]:
        """Find a table item"""
        tables_folder = self.find_folder_item("tables", schema)
        if not tables_folder:
            return None

        for i in range(tables_folder.childCount()):
            table_item = tables_folder.child(i)
            if (
                isinstance(table_item, DatabaseTreeItem)
                and table_item.object_name == table
            ):
                return table_item
        return None

    def on_item_expanded(self, item: QTreeWidgetItem):
        """Handle item expansion"""
        if not isinstance(item, DatabaseTreeItem):
            return

        if item.needs_loading():
            if item.object_type == DatabaseObjectType.SCHEMA:
                # For PostgreSQL databases, check if this is a database that needs schema loading
                connection = connection_manager.get_connection(item.connection_name) if item.connection_name else None
                if connection and hasattr(connection, 'profile') and hasattr(connection.profile, 'database_type'):
                    from .config import DatabaseType
                    if connection.profile.database_type == DatabaseType.POSTGRESQL:
                        # Check if parent is a connection (meaning this is a database, not a schema)
                        parent = item.parent()
                        if (isinstance(parent, DatabaseTreeItem) and 
                            parent.object_type == DatabaseObjectType.CONNECTION):
                            # This is a database under a PostgreSQL connection
                            self.load_database_schemas(item)
                            return

            elif item.object_type == DatabaseObjectType.FOLDER:
                folder_type = item.extra_data.get("folder_type")
                if folder_type == "tables":
                    # Load tables
                    worker = DatabaseBrowserWorker(
                        item.connection_name, "load_tables", schema=item.schema_name
                    )
                    worker.signals.tables_loaded.connect(self.on_tables_loaded)
                    worker.signals.error.connect(
                        lambda error: self.on_load_error(item, error)
                    )
                    self.thread_pool.start(worker)

            elif item.object_type == DatabaseObjectType.TABLE:
                # Load columns
                worker = DatabaseBrowserWorker(
                    item.connection_name,
                    "load_columns",
                    schema=item.schema_name,
                    table=item.object_name,
                )
                worker.signals.columns_loaded.connect(self.on_columns_loaded)
                worker.signals.error.connect(
                    lambda error: self.on_load_error(item, error)
                )
                self.thread_pool.start(worker)

    def on_item_clicked(self, item: QTreeWidgetItem, column: int):
        """Handle item selection"""
        if isinstance(item, DatabaseTreeItem):
            self.object_selected.emit(item)

    def on_item_double_clicked(self, item: QTreeWidgetItem, column: int):
        """Handle item double-click"""
        if isinstance(item, DatabaseTreeItem):
            # Special handling for connections: connect if not already connected
            if item.object_type == DatabaseObjectType.CONNECTION:
                connection = self.connections.get(item.connection_name)
                if connection and not connection.is_connected:
                    self.toggle_connection(item.connection_name)
                else:
                    # If already connected, just expand
                    item.setExpanded(not item.isExpanded())
            else:
                self.object_double_clicked.emit(item)

                # Auto-expand for containers
                if item.object_type in [
                    DatabaseObjectType.SCHEMA,
                    DatabaseObjectType.FOLDER,
                    DatabaseObjectType.TABLE,
                ]:
                    item.setExpanded(not item.isExpanded())

    def show_context_menu(self, position):
        """Show context menu for tree items"""
        item = self.tree.itemAt(position)
        if not isinstance(item, DatabaseTreeItem):
            return

        menu = QMenu(self)

        if item.object_type == DatabaseObjectType.CONNECTION:
            connect_action = QAction(
                "Connect"
                if not self.connections[item.connection_name].is_connected
                else "Disconnect",
                menu,
            )
            connect_action.triggered.connect(
                lambda: self.toggle_connection(item.connection_name)
            )
            menu.addAction(connect_action)

            menu.addSeparator()
            
            # Add Edit Connection option
            edit_action = QAction("Edit Connection...", menu)
            edit_action.triggered.connect(
                lambda: self.edit_connection(item.connection_name)
            )
            menu.addAction(edit_action)
            
            # Add Delete Connection option
            delete_action = QAction("Delete Connection...", menu)
            delete_action.triggered.connect(
                lambda: self.delete_connection(item.connection_name)
            )
            menu.addAction(delete_action)

            menu.addSeparator()
            refresh_action = QAction("Refresh", menu)
            refresh_action.triggered.connect(
                lambda: self.refresh_connection(item.connection_name)
            )
            menu.addAction(refresh_action)

        elif item.object_type == DatabaseObjectType.TABLE:
            select_action = QAction("SELECT * FROM table", menu)
            select_action.triggered.connect(lambda: self.generate_select_query(item))
            menu.addAction(select_action)

            menu.addSeparator()
            refresh_action = QAction("Refresh", menu)
            refresh_action.triggered.connect(lambda: self.refresh_table(item))
            menu.addAction(refresh_action)

        elif item.object_type == DatabaseObjectType.SCHEMA:
            refresh_action = QAction("Refresh", menu)
            refresh_action.triggered.connect(lambda: self.refresh_schema(item))
            menu.addAction(refresh_action)

        if menu.actions():
            menu.exec(self.tree.mapToGlobal(position))

    def toggle_connection(self, connection_name: str):
        """Toggle database connection"""
        connection = self.connections.get(connection_name)
        if not connection:
            return

        if connection.is_connected:
            # Disconnect
            connection.disconnect_sync()
            self.refresh_connection(connection_name)
        else:
            # Connect
            try:
                success = connection.connect_sync()
                if success:
                    QTimer.singleShot(
                        1000, lambda: self.refresh_connection(connection_name)
                    )
            except Exception as e:
                print(f"Connection failed: {e}")

    def generate_select_query(self, table_item: DatabaseTreeItem):
        """Generate SELECT query for table"""
        if table_item.schema_name and table_item.object_name:
            query = f"SELECT * FROM `{table_item.schema_name}`.`{table_item.object_name}` LIMIT 1000;"

            # Emit signal to parent to create new query tab
            # For now, copy to clipboard and show message
            from PyQt6.QtWidgets import QApplication

            clipboard = QApplication.clipboard()
            clipboard.setText(query)

            QMessageBox.information(
                self,
                "Query Generated",
                f"Query copied to clipboard:\n{query}\n\nCreate a new query tab (Ctrl+T) and paste to execute.",
            )

    def refresh_table(self, table_item: DatabaseTreeItem):
        """Refresh table structure"""
        table_item.takeChildren()
        table_item.loaded = False
        if table_item.isExpanded():
            self.on_item_expanded(table_item)

    def refresh_schema(self, schema_item: DatabaseTreeItem):
        """Refresh schema objects"""
        schema_item.takeChildren()
        schema_item.loaded = False
        if schema_item.isExpanded():
            self.on_item_expanded(schema_item)

    def edit_connection(self, connection_name: str):
        """Edit the selected connection profile"""
        from .config import settings
        from .gui import ConnectionDialog
        
        connection_profile = settings.get_connection(connection_name)
        if not connection_profile:
            QMessageBox.warning(self, "Error", f"Connection profile '{connection_name}' not found.")
            return
        
        # Open connection dialog in edit mode
        dialog = ConnectionDialog(self, connection_profile)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Update the connection profile
            updated_profile = dialog.get_connection_profile()
            
            # Remove old connection
            settings.remove_connection(connection_name)
            
            # Add updated connection
            settings.add_connection(updated_profile)
            
            # Update connection manager
            from .database import connection_manager
            if connection_name in connection_manager.connections:
                # Close old connection
                old_connection = connection_manager.connections[connection_name]
                if old_connection.is_connected:
                    try:
                        asyncio.run(old_connection.disconnect())
                    except:
                        pass
                del connection_manager.connections[connection_name]
            
            # Add new connection
            connection_manager.add_connection(updated_profile.name, updated_profile)
            
            # Refresh displays
            self.refresh_connections()

    def delete_connection(self, connection_name: str):
        """Delete the selected connection profile"""
        # Confirm deletion
        reply = QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Are you sure you want to delete the connection '{connection_name}'?\n\n"
            "This action cannot be undone.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            from .config import settings
            from .database import connection_manager
            
            # Disconnect if currently connected
            connection = connection_manager.get_connection(connection_name)
            if connection and connection.is_connected:
                try:
                    asyncio.run(connection.disconnect())
                except:
                    pass
            
            # Remove from connection manager
            if connection_name in connection_manager.connections:
                del connection_manager.connections[connection_name]
            
            # Remove from settings
            settings.remove_connection(connection_name)
            
            # Refresh displays
            self.refresh_connections()
            
            QMessageBox.information(self, "Connection Deleted", f"Connection '{connection_name}' has been deleted.")

    def filter_objects(self, filter_text: str):
        """Filter tree items by text"""
        # Simple filtering implementation
        for i in range(self.tree.topLevelItemCount()):
            conn_item = self.tree.topLevelItem(i)
            self.filter_item_recursive(conn_item, filter_text.lower())

    def filter_item_recursive(self, item: QTreeWidgetItem, filter_text: str):
        """Recursively filter tree items"""
        if not filter_text:
            item.setHidden(False)
            for i in range(item.childCount()):
                self.filter_item_recursive(item.child(i), filter_text)
            return

        # Check if item text matches filter
        item_text = item.text(0).lower()
        matches = filter_text in item_text

        # Check children
        child_matches = False
        for i in range(item.childCount()):
            child = item.child(i)
            self.filter_item_recursive(child, filter_text)
            if not child.isHidden():
                child_matches = True

        # Hide item if neither it nor its children match
        item.setHidden(not matches and not child_matches)
