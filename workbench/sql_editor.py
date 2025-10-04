"""
Advanced SQL editor with syntax highlighting and query execution.
"""

import asyncio
import re
from typing import Any, Dict, List, Optional

from PyQt6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    Qt,
    QTimer,
    QVariant,
    pyqtSignal,
)
from PyQt6.QtGui import (
    QColor,
    QFont,
    QKeySequence,
    QSyntaxHighlighter,
    QTextCharFormat,
    QTextDocument,
)
from PyQt6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

try:
    # Try to import QScintilla for advanced editing
    from PyQt6.Qsci import QsciAPIs, QsciLexerSQL, QsciScintilla

    QSCINTILLA_AVAILABLE = True
except ImportError:
    QSCINTILLA_AVAILABLE = False

from .database import QueryResult, QueryResultType, connection_manager


class SQLSyntaxHighlighter(QSyntaxHighlighter):
    """SQL syntax highlighter for QTextEdit"""

    def __init__(self, document: QTextDocument):
        super().__init__(document)
        self.setup_highlighting_rules()

    def setup_highlighting_rules(self):
        """Setup syntax highlighting rules"""
        self.highlighting_rules = []

        # Keywords
        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor(0, 0, 255))
        keyword_format.setFontWeight(QFont.Weight.Bold)

        keywords = [
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "TABLE",
            "INDEX",
            "VIEW",
            "AND",
            "OR",
            "NOT",
            "NULL",
            "IS",
            "IN",
            "LIKE",
            "JOIN",
            "LEFT",
            "RIGHT",
            "INNER",
            "OUTER",
            "ON",
            "GROUP",
            "BY",
            "ORDER",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "DISTINCT",
            "AS",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "UNION",
            "ALL",
            "EXISTS",
            "BETWEEN",
            "ASC",
            "DESC",
        ]

        for keyword in keywords:
            pattern = f"\\b{keyword}\\b"
            self.highlighting_rules.append(
                (re.compile(pattern, re.IGNORECASE), keyword_format)
            )

        # String literals
        string_format = QTextCharFormat()
        string_format.setForeground(QColor(163, 21, 21))
        self.highlighting_rules.append((re.compile(r"'[^']*'"), string_format))
        self.highlighting_rules.append((re.compile(r'"[^"]*"'), string_format))

        # Numbers
        number_format = QTextCharFormat()
        number_format.setForeground(QColor(0, 128, 0))
        self.highlighting_rules.append((re.compile(r"\b\d+\.?\d*\b"), number_format))

        # Comments
        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor(128, 128, 128))
        comment_format.setFontItalic(True)

        # Single line comments
        self.highlighting_rules.append((re.compile(r"--[^\n]*"), comment_format))
        self.highlighting_rules.append((re.compile(r"#[^\n]*"), comment_format))

        # Multi-line comments
        self.highlighting_rules.append(
            (re.compile(r"/\*.*?\*/", re.DOTALL), comment_format)
        )

    def highlightBlock(self, text: str):
        """Apply highlighting to a block of text"""
        for pattern, format in self.highlighting_rules:
            for match in pattern.finditer(text):
                start, end = match.span()
                self.setFormat(start, end - start, format)


class SQLEditor(QWidget):
    """SQL editor widget with syntax highlighting"""

    query_executed = pyqtSignal(QueryResult)

    def __init__(self, connection_name: Optional[str] = None):
        super().__init__()
        self.connection_name = connection_name
        self.setup_ui()

    def setup_ui(self):
        """Setup the SQL editor UI"""
        layout = QVBoxLayout(self)

        # Toolbar
        toolbar_layout = QHBoxLayout()

        self.execute_btn = QPushButton("Execute")
        self.execute_btn.setShortcut(QKeySequence("Ctrl+Return"))
        self.execute_btn.clicked.connect(self.execute_query)

        self.execute_current_btn = QPushButton("Execute Current")
        self.execute_current_btn.setShortcut(QKeySequence("Ctrl+Shift+Return"))
        self.execute_current_btn.clicked.connect(self.execute_current_statement)

        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setEnabled(False)

        toolbar_layout.addWidget(self.execute_btn)
        toolbar_layout.addWidget(self.execute_current_btn)
        toolbar_layout.addWidget(self.stop_btn)
        toolbar_layout.addStretch()

        # Connection selector
        self.connection_label = QLabel("No connection")
        toolbar_layout.addWidget(self.connection_label)

        layout.addLayout(toolbar_layout)

        # Main splitter
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        layout.addWidget(self.splitter)

        # SQL Editor
        if QSCINTILLA_AVAILABLE:
            self.editor = QsciScintilla()
            self.setup_qscintilla_editor()
        else:
            self.editor = QTextEdit()
            self.setup_basic_editor()

        self.splitter.addWidget(self.editor)

        # Results area
        self.results_widget = SQLResultsWidget()
        self.splitter.addWidget(self.results_widget)

        # Set initial splitter sizes
        self.splitter.setSizes([400, 300])

        # Update connection display
        self.update_connection_display()

    def setup_qscintilla_editor(self):
        """Setup QScintilla editor with advanced features"""
        # Set lexer for SQL syntax highlighting
        lexer = QsciLexerSQL()
        self.editor.setLexer(lexer)

        # Configure editor
        self.editor.setFont(QFont("Consolas", 10))
        self.editor.setTabWidth(4)
        self.editor.setIndentationsUseTabs(False)
        self.editor.setAutoIndent(True)
        self.editor.setBraceMatching(QsciScintilla.BraceMatch.SloppyBraceMatch)

        # Line numbers
        self.editor.setMarginType(0, QsciScintilla.MarginType.NumberMargin)
        self.editor.setMarginWidth(0, "0000")
        self.editor.setMarginLineNumbers(0, True)

        # Current line highlighting
        self.editor.setCaretLineVisible(True)
        self.editor.setCaretLineBackgroundColor(QColor("#ffe4e1"))

        # Set up auto-completion
        api = QsciAPIs(lexer)

        # Add SQL keywords and functions for auto-completion
        sql_keywords = [
            "SELECT",
            "FROM",
            "WHERE",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE TABLE",
            "DROP TABLE",
            "ALTER TABLE",
            "INDEX",
            "SUM",
            "COUNT",
            "AVG",
            "MIN",
            "MAX",
            "GROUP BY",
            "ORDER BY",
            "LEFT JOIN",
            "RIGHT JOIN",
            "INNER JOIN",
            "OUTER JOIN",
        ]

        for keyword in sql_keywords:
            api.add(keyword)

        api.prepare()

        # Enable auto-completion
        self.editor.setAutoCompletionSource(QsciScintilla.AutoCompletionSource.AcsAll)
        self.editor.setAutoCompletionThreshold(2)

    def setup_basic_editor(self):
        """Setup basic QTextEdit with syntax highlighting"""
        self.editor.setFont(QFont("Consolas", 10))

        # Apply syntax highlighter
        self.highlighter = SQLSyntaxHighlighter(self.editor.document())

        # Set placeholder
        self.editor.setPlaceholderText("-- Enter your SQL query here...")

    def set_connection(self, connection_name: str):
        """Set the database connection for this editor"""
        self.connection_name = connection_name
        self.update_connection_display()

    def update_connection_display(self):
        """Update connection display"""
        if self.connection_name:
            connection = connection_manager.get_connection(self.connection_name)
            if connection and connection.is_connected:
                self.connection_label.setText(f"Connected: {self.connection_name}")
                self.execute_btn.setEnabled(True)
                self.execute_current_btn.setEnabled(True)
            else:
                self.connection_label.setText(f"Disconnected: {self.connection_name}")
                self.execute_btn.setEnabled(False)
                self.execute_current_btn.setEnabled(False)
        else:
            self.connection_label.setText("No connection")
            self.execute_btn.setEnabled(False)
            self.execute_current_btn.setEnabled(False)

    def get_current_text(self) -> str:
        """Get text from editor"""
        if hasattr(self.editor, "text"):  # QsciScintilla
            return self.editor.text()
        else:  # QTextEdit
            return self.editor.toPlainText()

    def set_text(self, text: str):
        """Set text in editor"""
        if hasattr(self.editor, "setText"):  # QsciScintilla
            self.editor.setText(text)
        else:  # QTextEdit
            self.editor.setPlainText(text)

    def get_selected_text(self) -> str:
        """Get selected text from editor"""
        if hasattr(self.editor, "selectedText"):  # QsciScintilla
            return self.editor.selectedText()
        else:  # QTextEdit
            cursor = self.editor.textCursor()
            return cursor.selectedText()

    def execute_query(self):
        """Execute all SQL in the editor"""
        sql = self.get_current_text().strip()
        if not sql:
            return

        self._execute_sql(sql)

    def execute_current_statement(self):
        """Execute current SQL statement"""
        selected = self.get_selected_text().strip()
        if selected:
            self._execute_sql(selected)
        else:
            # Try to find current statement
            sql = self.get_current_text().strip()
            if sql:
                self._execute_sql(sql)

    def _execute_sql(self, sql: str):
        """Execute SQL query"""
        if not self.connection_name:
            QMessageBox.warning(
                self, "No Connection", "Please select a database connection."
            )
            return

        connection = connection_manager.get_connection(self.connection_name)
        if not connection or not connection.is_connected:
            QMessageBox.warning(
                self, "Not Connected", "Database connection is not established."
            )
            return

        # Disable buttons during execution
        self.execute_btn.setEnabled(False)
        self.execute_current_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        # Execute in thread (simplified - in real app would use proper async)
        QTimer.singleShot(100, lambda: self._execute_sql_real(sql))

    def _execute_sql_real(self, sql: str):
        """Actually execute SQL query against database"""
        try:
            connection = connection_manager.get_connection(self.connection_name)
            if not connection or not connection.is_connected:
                error_result = QueryResult(
                    result_type=QueryResultType.ERROR,
                    error_message="Database connection is not available",
                    execution_time=0.0,
                )
                self.results_widget.show_result(error_result)
                return

            # Execute the query using the real database connection
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                result = loop.run_until_complete(
                    connection.execute_query(sql, fetch_results=True)
                )
            finally:
                loop.close()

            # Show result in UI
            self.results_widget.show_result(result)
            self.query_executed.emit(result)

        except Exception as e:
            error_result = QueryResult(
                result_type=QueryResultType.ERROR,
                error_message=f"Query execution failed: {str(e)}",
                execution_time=0.0,
            )
            self.results_widget.show_result(error_result)

        finally:
            # Re-enable buttons after execution
            self.execute_btn.setEnabled(True)
            self.execute_current_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)


class ResultTableModel(QAbstractTableModel):
    """Table model for query results"""

    def __init__(self, data: List[Dict[str, Any]], columns: List[Dict[str, Any]]):
        super().__init__()
        self._data = data
        self._columns = columns

    def rowCount(self, parent=QModelIndex()):
        return len(self._data)

    def columnCount(self, parent=QModelIndex()):
        return len(self._columns)

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return QVariant()

        if role == Qt.ItemDataRole.DisplayRole:
            row = self._data[index.row()]
            column_name = self._columns[index.column()]["name"]
            return str(row.get(column_name, ""))

        return QVariant()

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role=Qt.ItemDataRole.DisplayRole,
    ):
        if (
            orientation == Qt.Orientation.Horizontal
            and role == Qt.ItemDataRole.DisplayRole
        ):
            return self._columns[section]["name"]

        if (
            orientation == Qt.Orientation.Vertical
            and role == Qt.ItemDataRole.DisplayRole
        ):
            return str(section + 1)

        return QVariant()


class SQLResultsWidget(QWidget):
    """Widget for displaying SQL query results"""

    def __init__(self):
        super().__init__()
        self.setup_ui()

    def setup_ui(self):
        """Setup results widget UI"""
        layout = QVBoxLayout(self)

        # Results tabs
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)

        # Status bar
        status_layout = QHBoxLayout()
        self.status_label = QLabel("Ready")
        self.row_count_label = QLabel("")
        self.execution_time_label = QLabel("")

        status_layout.addWidget(self.status_label)
        status_layout.addStretch()
        status_layout.addWidget(self.row_count_label)
        status_layout.addWidget(self.execution_time_label)

        layout.addLayout(status_layout)

    def show_result(self, result: QueryResult):
        """Display query result"""
        self.clear_results()

        if result.result_type == QueryResultType.RESULTSET:
            self.show_resultset(result)
        elif result.result_type == QueryResultType.UPDATE:
            self.show_update_result(result)
        elif result.result_type == QueryResultType.ERROR:
            self.show_error(result)

    def show_resultset(self, result: QueryResult):
        """Display SELECT query results"""
        if not result.data or not result.columns:
            self.status_label.setText("Query returned no results")
            return

        # Create table widget
        table = QTableWidget()
        table.setRowCount(len(result.data))
        table.setColumnCount(len(result.columns))

        # Set headers
        headers = [col["name"] for col in result.columns]
        table.setHorizontalHeaderLabels(headers)

        # Populate data
        for row_idx, row_data in enumerate(result.data):
            for col_idx, column in enumerate(result.columns):
                col_name = column["name"]
                value = row_data.get(col_name, "")
                item = QTableWidgetItem(str(value))
                table.setItem(row_idx, col_idx, item)

        # Configure table
        table.resizeColumnsToContents()
        table.setAlternatingRowColors(True)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)

        # Add to tabs
        self.tab_widget.addTab(table, "Result")

        # Update status
        self.row_count_label.setText(f"{len(result.data)} rows")
        self.execution_time_label.setText(f"{result.execution_time:.3f}s")
        self.status_label.setText(
            f"Query executed successfully - {len(result.data)} rows returned"
        )

    def show_update_result(self, result: QueryResult):
        """Display UPDATE/INSERT/DELETE results"""
        message = result.message or f"{result.affected_rows} rows affected"

        # Update status
        self.execution_time_label.setText(f"{result.execution_time:.3f}s")
        self.status_label.setText(f"Query executed successfully - {message}")

    def show_error(self, result: QueryResult):
        """Display error message"""
        error_msg = f"Error: {result.error_message}"
        if result.error_code:
            error_msg = f"Error {result.error_code}: {result.error_message}"

        self.status_label.setText(f"Query failed - {error_msg}")

        if result.execution_time:
            self.execution_time_label.setText(f"{result.execution_time:.3f}s")

    def clear_results(self):
        """Clear all results"""
        self.tab_widget.clear()
        self.row_count_label.clear()
        self.execution_time_label.clear()
        self.status_label.setText("Ready")
