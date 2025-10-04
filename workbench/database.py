"""
Database connection and query execution engine.
"""

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import pymysql
import pymysql.cursors
from pymysql.err import Error as PyMySQLError

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import Error as PostgreSQLError

    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
import socket
import threading
from abc import ABC, abstractmethod
from datetime import datetime

import paramiko

from .config import ConnectionProfile, DatabaseType

logger = logging.getLogger(__name__)


class QueryResultType(Enum):
    """Types of query results"""

    RESULTSET = "resultset"
    UPDATE = "update"
    ERROR = "error"
    MESSAGE = "message"


@dataclass
class QueryResult:
    """Result of a SQL query execution"""

    result_type: QueryResultType
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[Dict[str, Any]]] = None
    affected_rows: int = 0
    execution_time: float = 0.0
    error_message: Optional[str] = None
    error_code: Optional[int] = None
    message: Optional[str] = None


@dataclass
class DatabaseObject:
    """Represents a database object (schema, table, column, etc.)"""

    name: str
    object_type: str  # schema, table, view, procedure, function, trigger
    parent: Optional[str] = None
    schema: Optional[str] = None
    comment: Optional[str] = None
    extra_info: Optional[Dict[str, Any]] = None


class SSHTunnel:
    """SSH tunnel for secure database connections"""

    def __init__(
        self,
        ssh_host: str,
        ssh_port: int,
        ssh_username: str,
        ssh_key_file: Optional[str] = None,
        ssh_password: Optional[str] = None,
        remote_host: str = "localhost",
        remote_port: int = 3306,
    ):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_key_file = ssh_key_file
        self.ssh_password = ssh_password
        self.remote_host = remote_host
        self.remote_port = remote_port

        self.ssh_client = None
        self.tunnel = None
        self.local_port = None

    def start(self) -> int:
        """Start SSH tunnel and return local port"""
        try:
            # Create SSH client
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to SSH server with timeout
            connect_kwargs = {
                "hostname": self.ssh_host,
                "port": self.ssh_port,
                "username": self.ssh_username,
                "timeout": 10,  # 10 second timeout for SSH connection
            }

            if self.ssh_key_file:
                connect_kwargs["key_filename"] = self.ssh_key_file
            else:
                connect_kwargs["password"] = self.ssh_password

            self.ssh_client.connect(**connect_kwargs)

            # Create tunnel
            transport = self.ssh_client.get_transport()
            if not transport:
                raise Exception("Failed to get SSH transport")

            # Find available local port
            sock = socket.socket()
            sock.bind(("", 0))
            self.local_port = sock.getsockname()[1]
            sock.close()

            # Start port forwarding
            self.tunnel = transport.open_channel(
                "direct-tcpip",
                (self.remote_host, self.remote_port),
                ("localhost", self.local_port),
            )

            if not self.tunnel:
                raise Exception(
                    f"Failed to open SSH tunnel to {self.remote_host}:{self.remote_port}"
                )

            return self.local_port

        except paramiko.AuthenticationException as e:
            logger.error(f"SSH authentication failed: {e}")
            raise Exception("SSH authentication failed: Check username/password/key")
        except (paramiko.SSHException, socket.error) as e:
            logger.error(f"SSH connection failed: {e}")
            raise Exception(
                f"Cannot connect to SSH server {self.ssh_host}:{self.ssh_port}"
            )
        except Exception as e:
            logger.error(f"Failed to create SSH tunnel: {e}")
            raise Exception(f"SSH tunnel error: {str(e)}")

    def stop(self):
        """Stop SSH tunnel"""
        if self.tunnel:
            self.tunnel.close()
        if self.ssh_client:
            self.ssh_client.close()


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters"""

    def __init__(self, profile: ConnectionProfile):
        self.profile = profile
        self.connection = None
        self.is_connected = False
        self.ssh_tunnel = None

    @abstractmethod
    def connect_sync(self) -> bool:
        """Synchronous connection method"""
        pass

    @abstractmethod
    async def connect(self) -> bool:
        """Asynchronous connection method"""
        pass

    @abstractmethod
    async def disconnect(self):
        """Disconnect from database"""
        pass

    @abstractmethod
    async def execute_query(
        self, query: str, fetch_results: bool = True
    ) -> QueryResult:
        """Execute SQL query"""
        pass

    @abstractmethod
    async def get_databases(self) -> List[DatabaseObject]:
        """Get list of databases/schemas"""
        pass

    async def get_schemas(self, database: str) -> List[DatabaseObject]:
        """Get list of schemas in database (default implementation returns empty list)"""
        return []

    @abstractmethod
    async def get_tables(self, schema: str) -> List[DatabaseObject]:
        """Get list of tables in schema"""
        pass

    @abstractmethod
    async def get_table_columns(self, schema: str, table: str) -> List[DatabaseObject]:
        """Get columns for a specific table"""
        pass

    @abstractmethod
    def test_connection_sync(self) -> tuple[bool, str]:
        """Test connection synchronously"""
        pass


class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter"""

    def __init__(self, profile: ConnectionProfile):
        super().__init__(profile)

    def connect_sync(self) -> bool:
        """Connect to MySQL database synchronously"""
        try:
            # Handle SSH tunnel if needed
            connect_params = {
                "host": self.profile.host,
                "port": self.profile.port,
                "user": self.profile.username,
                "password": self.profile.password,
                "charset": "utf8mb4",
                "connect_timeout": 10,
                "read_timeout": 30,
                "write_timeout": 30,
            }

            if self.profile.ssh_hostname:
                self.ssh_tunnel = SSHTunnel(
                    ssh_host=self.profile.ssh_hostname,
                    ssh_port=self.profile.ssh_port or 22,
                    ssh_username=self.profile.ssh_username,
                    ssh_key_file=str(self.profile.ssh_key_file)
                    if self.profile.ssh_key_file
                    else None,
                    remote_host=self.profile.host,
                    remote_port=self.profile.port,
                )
                local_port = self.ssh_tunnel.start()
                connect_params["host"] = "localhost"
                connect_params["port"] = local_port

            # Add default database if specified
            if self.profile.default_schema:
                connect_params["database"] = self.profile.default_schema

            self.connection = pymysql.connect(**connect_params)
            self.is_connected = True
            return True

        except Exception as e:
            logger.error(f"MySQL connection failed: {e}")
            self.is_connected = False
            if self.ssh_tunnel:
                self.ssh_tunnel.stop()
                self.ssh_tunnel = None
            return False

    async def connect(self) -> bool:
        """Connect to MySQL database asynchronously"""
        return await asyncio.get_event_loop().run_in_executor(None, self.connect_sync)

    async def disconnect(self):
        """Disconnect from MySQL database"""
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            self.connection = None

        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            self.ssh_tunnel = None

        self.is_connected = False

    def disconnect_sync(self):
        """Disconnect synchronously"""
        asyncio.run(self.disconnect())

    async def execute_query(
        self, query: str, fetch_results: bool = True
    ) -> QueryResult:
        """Execute MySQL query"""
        if not self.connection or not self.is_connected:
            return QueryResult(
                result_type=QueryResultType.ERROR,
                error_message="Not connected to database",
                execution_time=0.0,
            )

        start_time = datetime.now()

        try:
            with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query)

                execution_time = (datetime.now() - start_time).total_seconds()

                if fetch_results and cursor.description:
                    # SELECT query with results
                    rows = cursor.fetchall()
                    columns = [
                        {"name": desc[0], "type": str(desc[1]), "nullable": True}
                        for desc in cursor.description
                    ]

                    return QueryResult(
                        result_type=QueryResultType.RESULTSET,
                        data=rows,
                        columns=columns,
                        execution_time=execution_time,
                    )
                else:
                    # UPDATE/INSERT/DELETE query
                    affected_rows = cursor.rowcount
                    self.connection.commit()

                    return QueryResult(
                        result_type=QueryResultType.UPDATE,
                        affected_rows=affected_rows,
                        execution_time=execution_time,
                        message=f"{affected_rows} row(s) affected",
                    )

        except PyMySQLError as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return QueryResult(
                result_type=QueryResultType.ERROR,
                error_message=str(e),
                error_code=getattr(e, "args", [None])[0],
                execution_time=execution_time,
            )

    async def get_databases(self) -> List[DatabaseObject]:
        """Get list of MySQL databases"""
        result = await self.execute_query("SHOW DATABASES")
        databases = []

        if result.result_type == QueryResultType.RESULTSET and result.data:
            for row in result.data:
                db_name = row.get("Database", "")
                databases.append(DatabaseObject(name=db_name, object_type="schema"))

        return databases

    async def get_schemas(self, database: str) -> List[DatabaseObject]:
        """MySQL doesn't have schemas like PostgreSQL - return empty list"""
        return []

    async def get_tables(self, schema: str) -> List[DatabaseObject]:
        """Get list of tables in MySQL schema"""
        query = f"SHOW TABLES FROM `{schema}`"
        result = await self.execute_query(query)
        tables = []

        if result.result_type == QueryResultType.RESULTSET and result.data:
            for row in result.data:
                # The column name varies by MySQL version
                table_name = list(row.values())[0] if row else ""
                if table_name:
                    tables.append(
                        DatabaseObject(
                            name=table_name, object_type="table", schema=schema
                        )
                    )

        return tables

    async def get_table_columns(self, schema: str, table: str) -> List[DatabaseObject]:
        """Get columns for MySQL table"""
        query = f"DESCRIBE `{schema}`.`{table}`"
        result = await self.execute_query(query)
        columns = []

        if result.result_type == QueryResultType.RESULTSET and result.data:
            for row in result.data:
                columns.append(
                    DatabaseObject(
                        name=row.get("Field", ""),
                        object_type="column",
                        schema=schema,
                        parent=table,
                        extra_info={
                            "data_type": row.get("Type", ""),
                            "nullable": row.get("Null", "") == "YES",
                            "key": row.get("Key", ""),
                            "default": row.get("Default", ""),
                            "extra": row.get("Extra", ""),
                        },
                    )
                )

        return columns

    def test_connection_sync(self) -> tuple[bool, str]:
        """Test MySQL connection synchronously"""
        try:
            success = self.connect_sync()
            if success:
                # Test with a simple query
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                self.disconnect_sync()
                return True, "Connection successful"
            else:
                return False, "Connection failed"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL database adapter"""

    def __init__(self, profile: ConnectionProfile):
        super().__init__(profile)
        if not POSTGRESQL_AVAILABLE:
            raise ImportError(
                "psycopg2 is not installed. Install with: pip install psycopg2-binary"
            )

    def connect_sync(self) -> bool:
        """Connect to PostgreSQL database synchronously"""
        try:
            # Handle SSH tunnel if needed
            connect_params = {
                "host": self.profile.host,
                "port": self.profile.port,
                "user": self.profile.username,
                "password": self.profile.password,
                "connect_timeout": 10,
            }

            if self.profile.ssh_hostname:
                self.ssh_tunnel = SSHTunnel(
                    ssh_host=self.profile.ssh_hostname,
                    ssh_port=self.profile.ssh_port or 22,
                    ssh_username=self.profile.ssh_username,
                    ssh_key_file=str(self.profile.ssh_key_file)
                    if self.profile.ssh_key_file
                    else None,
                    remote_host=self.profile.host,
                    remote_port=self.profile.port,
                )
                local_port = self.ssh_tunnel.start()
                connect_params["host"] = "localhost"
                connect_params["port"] = local_port

            # Add default database if specified
            if self.profile.default_schema:
                connect_params["database"] = self.profile.default_schema
            else:
                connect_params["database"] = "postgres"  # Default PostgreSQL database

            self.connection = psycopg2.connect(**connect_params)
            self.connection.autocommit = True  # Enable autocommit for PostgreSQL
            self.is_connected = True
            return True

        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            self.is_connected = False
            if self.ssh_tunnel:
                self.ssh_tunnel.stop()
                self.ssh_tunnel = None
            return False

    async def connect(self) -> bool:
        """Connect to PostgreSQL database asynchronously"""
        return await asyncio.get_event_loop().run_in_executor(None, self.connect_sync)

    async def disconnect(self):
        """Disconnect from PostgreSQL database"""
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            self.connection = None

        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            self.ssh_tunnel = None

        self.is_connected = False

    def disconnect_sync(self):
        """Disconnect synchronously"""
        asyncio.run(self.disconnect())

    async def execute_query(
        self, query: str, fetch_results: bool = True
    ) -> QueryResult:
        """Execute PostgreSQL query"""
        if not self.connection or not self.is_connected:
            return QueryResult(
                result_type=QueryResultType.ERROR,
                error_message="Not connected to database",
                execution_time=0.0,
            )

        start_time = datetime.now()

        try:
            with self.connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                cursor.execute(query)

                execution_time = (datetime.now() - start_time).total_seconds()

                if fetch_results and cursor.description:
                    # SELECT query with results
                    rows = [dict(row) for row in cursor.fetchall()]
                    columns = [
                        {
                            "name": desc.name,
                            "type": str(desc.type_code),
                            "nullable": True,
                        }
                        for desc in cursor.description
                    ]

                    return QueryResult(
                        result_type=QueryResultType.RESULTSET,
                        data=rows,
                        columns=columns,
                        execution_time=execution_time,
                    )
                else:
                    # UPDATE/INSERT/DELETE query
                    affected_rows = cursor.rowcount

                    return QueryResult(
                        result_type=QueryResultType.UPDATE,
                        affected_rows=affected_rows,
                        execution_time=execution_time,
                        message=f"{affected_rows} row(s) affected",
                    )

        except PostgreSQLError as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return QueryResult(
                result_type=QueryResultType.ERROR,
                error_message=str(e),
                error_code=getattr(e, "pgcode", None),
                execution_time=execution_time,
            )

    async def get_databases(self) -> List[DatabaseObject]:
        """Get list of PostgreSQL databases"""
        logger.info("PostgreSQL: Getting databases")
        result = await self.execute_query(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        )
        databases = []

        if result.result_type == QueryResultType.RESULTSET and result.data:
            for row in result.data:
                db_name = row.get("datname", "")
                databases.append(DatabaseObject(name=db_name, object_type="database"))
        elif result.result_type == QueryResultType.ERROR:
            logger.error(f"PostgreSQL get_databases error: {result.error_message}")

        logger.info(f"PostgreSQL: Found {len(databases)} databases")
        return databases

    async def get_schemas(self, database: str) -> List[DatabaseObject]:
        """Get list of schemas in a PostgreSQL database"""
        logger.info(f"PostgreSQL: Getting schemas in database '{database}'")

        # For PostgreSQL, we need to connect to the specific database to see its schemas
        temp_connection = None
        try:
            # Create temporary connection to the target database
            connect_params = {
                "host": self.profile.host,
                "port": self.profile.port,
                "user": self.profile.username,
                "password": self.profile.password,
                "database": database,
                "connect_timeout": 10,
            }

            # Handle SSH tunnel if needed (reuse existing tunnel)
            if self.ssh_tunnel and hasattr(self.ssh_tunnel, "local_port"):
                connect_params["host"] = "localhost"
                connect_params["port"] = self.ssh_tunnel.local_port

            temp_connection = psycopg2.connect(**connect_params)

            query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
                ORDER BY schema_name
            """

            with temp_connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                cursor.execute(query)
                rows = [dict(row) for row in cursor.fetchall()]

            schemas = []
            for row in rows:
                schema_name = row.get("schema_name", "")
                if schema_name:
                    schemas.append(
                        DatabaseObject(
                            name=schema_name, object_type="schema", parent=database
                        )
                    )

            logger.info(
                f"PostgreSQL: Found {len(schemas)} schemas in database '{database}'"
            )
            return schemas

        except PostgreSQLError as e:
            logger.error(f"PostgreSQL get_schemas error for database '{database}': {e}")
            return []
        except Exception as e:
            logger.error(
                f"PostgreSQL get_schemas unexpected error for database '{database}': {e}"
            )
            return []
        finally:
            if temp_connection:
                try:
                    temp_connection.close()
                except:
                    pass

    async def get_tables(self, schema: str) -> List[DatabaseObject]:
        """Get list of tables in PostgreSQL schema"""
        # Extract database and schema if schema contains database.schema format
        if "." in schema:
            database, schema_name = schema.split(".", 1)
        else:
            # If no database specified, try to get from connection or use current connection
            database = None
            schema_name = schema

        logger.info(
            f"PostgreSQL: Getting tables for schema '{schema_name}' in database '{database}'"
        )

        # If we have database context, use temporary connection to that database
        if database:
            return await self._get_tables_with_database_context(database, schema_name)
        else:
            # Fallback to current connection
            return await self._get_tables_current_connection(schema_name)

    async def _get_tables_with_database_context(
        self, database: str, schema: str
    ) -> List[DatabaseObject]:
        """Get tables using a temporary connection to specific database"""
        temp_connection = None
        try:
            # Create temporary connection to the target database
            connect_params = {
                "host": self.profile.host,
                "port": self.profile.port,
                "user": self.profile.username,
                "password": self.profile.password,
                "database": database,
                "connect_timeout": 10,
            }

            # Handle SSH tunnel if needed (reuse existing tunnel)
            if self.ssh_tunnel and hasattr(self.ssh_tunnel, "local_port"):
                connect_params["host"] = "localhost"
                connect_params["port"] = self.ssh_tunnel.local_port

            temp_connection = psycopg2.connect(**connect_params)

            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """

            with temp_connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                cursor.execute(query, (schema,))
                rows = [dict(row) for row in cursor.fetchall()]

            tables = []
            for row in rows:
                table_name = row.get("table_name", "")
                if table_name:
                    tables.append(
                        DatabaseObject(
                            name=table_name, object_type="table", schema=schema
                        )
                    )

            logger.info(
                f"PostgreSQL: Found {len(tables)} tables in schema '{database}.{schema}'"
            )
            return tables

        except PostgreSQLError as e:
            logger.error(
                f"PostgreSQL get_tables error for database '{database}', schema '{schema}': {e}"
            )
            return []
        except Exception as e:
            logger.error(
                f"PostgreSQL get_tables unexpected error for database '{database}', schema '{schema}': {e}"
            )
            return []
        finally:
            if temp_connection:
                try:
                    temp_connection.close()
                except:
                    pass

    async def _get_tables_current_connection(self, schema: str) -> List[DatabaseObject]:
        """Get tables using current connection (fallback method)"""
        logger.info(
            f"PostgreSQL: Getting tables for schema '{schema}' using current connection"
        )
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        if not self.connection or not self.is_connected:
            logger.warning("PostgreSQL: No active connection for get_tables")
            return []

        try:
            with self.connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                cursor.execute(query, (schema,))
                rows = [dict(row) for row in cursor.fetchall()]

            tables = []
            for row in rows:
                table_name = row.get("table_name", "")
                if table_name:
                    tables.append(
                        DatabaseObject(
                            name=table_name, object_type="table", schema=schema
                        )
                    )

            logger.info(
                f"PostgreSQL: Found {len(tables)} tables in schema '{schema}' (current connection)"
            )
            return tables

        except PostgreSQLError as e:
            logger.error(f"PostgreSQL get_tables error: {e}")
            return []
        except Exception as e:
            logger.error(f"PostgreSQL get_tables unexpected error: {e}")
            return []

    async def get_tables_old_method(self, schema: str) -> List[DatabaseObject]:
        """Get list of tables in PostgreSQL schema (old method using execute_query)"""
        logger.info(f"PostgreSQL: Getting tables for schema '{schema}' (old method)")
        query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        logger.debug(f"PostgreSQL query: {query.strip()}")
        result = await self.execute_query(query)
        logger.debug(
            f"PostgreSQL query result: {result.result_type}, rows: {len(result.data) if result.data else 0}"
        )
        tables = []

        if result.result_type == QueryResultType.RESULTSET and result.data:
            for row in result.data:
                table_name = row.get("table_name", "")
                if table_name:
                    tables.append(
                        DatabaseObject(
                            name=table_name, object_type="table", schema=schema
                        )
                    )
        elif result.result_type == QueryResultType.ERROR:
            logger.error(f"PostgreSQL get_tables error: {result.error_message}")

        logger.info(f"PostgreSQL: Found {len(tables)} tables in schema '{schema}'")
        return tables

    async def get_table_columns(self, schema: str, table: str) -> List[DatabaseObject]:
        """Get columns for PostgreSQL table"""
        logger.info(f"PostgreSQL: Getting columns for table '{schema}.{table}'")

        if not self.connection or not self.is_connected:
            return []

        try:
            query = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """

            with self.connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cursor:
                cursor.execute(query, (schema, table))
                rows = [dict(row) for row in cursor.fetchall()]

            columns = []
            for row in rows:
                columns.append(
                    DatabaseObject(
                        name=row.get("column_name", ""),
                        object_type="column",
                        schema=schema,
                        parent=table,
                        extra_info={
                            "data_type": row.get("data_type", ""),
                            "nullable": row.get("is_nullable", "") == "YES",
                            "default": row.get("column_default", ""),
                        },
                    )
                )

            logger.info(
                f"PostgreSQL: Found {len(columns)} columns in table '{schema}.{table}'"
            )
            return columns

        except PostgreSQLError as e:
            logger.error(f"PostgreSQL get_table_columns error: {e}")
            return []
        except Exception as e:
            logger.error(f"PostgreSQL get_table_columns unexpected error: {e}")
            return []

    async def get_table_columns_old_method(
        self, schema: str, table: str
    ) -> List[DatabaseObject]:
        """Get columns for PostgreSQL table (old method)"""
        query = f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position
        """
        result = await self.execute_query(query)
        columns = []

        if result.result_type == QueryResultType.RESULTSET and result.data:
            for row in result.data:
                columns.append(
                    DatabaseObject(
                        name=row.get("column_name", ""),
                        object_type="column",
                        schema=schema,
                        parent=table,
                        extra_info={
                            "data_type": row.get("data_type", ""),
                            "nullable": row.get("is_nullable", "") == "YES",
                            "default": row.get("column_default", ""),
                        },
                    )
                )

        return columns

    def test_connection_sync(self) -> tuple[bool, str]:
        """Test PostgreSQL connection synchronously"""
        try:
            success = self.connect_sync()
            if success:
                # Test with a simple query
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                self.disconnect_sync()
                return True, "Connection successful"
            else:
                return False, "Connection failed"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"


def create_database_adapter(profile: ConnectionProfile) -> DatabaseAdapter:
    """Factory method to create appropriate database adapter based on profile"""
    if profile.database_type == DatabaseType.MYSQL:
        return MySQLAdapter(profile)
    elif profile.database_type == DatabaseType.POSTGRESQL:
        return PostgreSQLAdapter(profile)
    else:
        raise ValueError(f"Unsupported database type: {profile.database_type}")


class DatabaseConnection:
    """Database connection wrapper that uses adapters for different database types"""

    def __init__(self, profile: ConnectionProfile):
        self.profile = profile
        self.adapter = create_database_adapter(profile)
        self.connection_lock = threading.Lock()

    @property
    def is_connected(self) -> bool:
        return self.adapter.is_connected

    async def connect(self) -> bool:
        """Establish database connection"""
        return await self.adapter.connect()

    def connect_sync(self) -> bool:
        """Synchronous database connection"""
        return self.adapter.connect_sync()

    async def disconnect(self):
        """Close database connection"""
        await self.adapter.disconnect()

    def disconnect_sync(self):
        """Synchronous version of disconnect for cleanup"""
        if hasattr(self.adapter, "disconnect_sync"):
            self.adapter.disconnect_sync()
        else:
            asyncio.run(self.adapter.disconnect())

    async def execute_query(self, sql: str, fetch_results: bool = True) -> QueryResult:
        """Execute SQL query and return results"""
        return await self.adapter.execute_query(sql, fetch_results)

    async def get_databases(self) -> List[DatabaseObject]:
        """Get list of databases/schemas"""
        databases = await self.adapter.get_databases()

        # Sort databases - user schemas first, then system schemas
        if self.profile.database_type == DatabaseType.MYSQL:
            user_schemas = []
            system_schemas = []

            for db in databases:
                if db.name in (
                    "information_schema",
                    "mysql",
                    "performance_schema",
                    "sys",
                ):
                    system_schemas.append(db)
                else:
                    user_schemas.append(db)

            return user_schemas + system_schemas

        return databases

    async def get_schemas(self, database: str) -> List[DatabaseObject]:
        """Get list of schemas in a database"""
        return await self.adapter.get_schemas(database)

    async def get_tables(self, schema: str) -> List[DatabaseObject]:
        """Get list of tables in a schema"""
        return await self.adapter.get_tables(schema)

    async def get_table_columns(self, schema: str, table: str) -> List[DatabaseObject]:
        """Get list of columns in a table"""
        return await self.adapter.get_table_columns(schema, table)

    async def test_connection(self) -> bool:
        """Test database connection"""
        try:
            result = await self.execute_query("SELECT 1 as test")
            return result.result_type == QueryResultType.RESULTSET
        except:
            return False

    def test_connection_sync(self) -> tuple[bool, str]:
        """Test database connection synchronously - returns (success, message)"""
        return self.adapter.test_connection_sync()


# For backward compatibility, keep MySQLConnection as an alias
MySQLConnection = DatabaseConnection


class ConnectionManager:
    """Manages multiple database connections"""

    def __init__(self):
        self.connections: Dict[str, DatabaseConnection] = {}

    def add_connection(
        self, name: str, profile: ConnectionProfile
    ) -> DatabaseConnection:
        """Add a new connection"""
        connection = DatabaseConnection(profile)
        self.connections[name] = connection
        return connection

    def get_connection(self, name: str) -> Optional[DatabaseConnection]:
        """Get connection by name"""
        return self.connections.get(name)

    def remove_connection(self, name: str):
        """Remove and disconnect a connection"""
        if name in self.connections:
            connection = self.connections[name]
            asyncio.create_task(connection.disconnect())
            del self.connections[name]

    async def disconnect_all(self):
        """Disconnect all connections"""
        for connection in self.connections.values():
            await connection.disconnect()
        self.connections.clear()


# Global connection manager
connection_manager = ConnectionManager()
