"""
Application configuration management using Pydantic settings.
"""

import json
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseType(str, Enum):
    """Supported database types"""

    MYSQL = "mysql"
    POSTGRESQL = "postgresql"


class ConnectionProfile(BaseModel):
    """Database connection profile"""

    name: str
    database_type: DatabaseType = DatabaseType.MYSQL
    host: str = "localhost"
    port: int = 3306
    username: str = ""
    password: str = ""  # Will be encrypted in real implementation
    default_schema: str = ""
    use_ssl: bool = False
    ssh_hostname: Optional[str] = None
    ssh_port: Optional[int] = 22
    ssh_username: Optional[str] = None
    ssh_key_file: Optional[Path] = None


class EditorSettings(BaseModel):
    """SQL Editor configuration"""

    font_family: str = "Consolas"
    font_size: int = 10
    tab_size: int = 4
    auto_indent: bool = True
    show_line_numbers: bool = True
    highlight_current_line: bool = True
    word_wrap: bool = False
    theme: str = "default"


class UISettings(BaseModel):
    """User interface settings"""

    theme: str = "system"  # system, light, dark
    window_geometry: Optional[Dict] = None
    splitter_states: Optional[Dict] = None
    recent_files: List[Path] = Field(default_factory=list)
    max_recent_files: int = 10


class ApplicationSettings(BaseSettings):
    """Main application settings"""

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="MYSQL_WB_", case_sensitive=False, extra="ignore"
    )

    # Application metadata
    app_name: str = "MySQL & PostgreSQL Workbench"
    version: str = "1.0.0"

    # File paths
    config_dir: Path = Field(default_factory=lambda: Path.home() / ".workbench")
    connections_file: Path = Field(
        default_factory=lambda: Path.home() / ".workbench" / "connections.json"
    )

    # Application settings
    auto_save_interval: int = 300  # seconds
    max_query_history: int = 1000
    default_result_limit: int = 1000

    # UI Configuration
    ui: UISettings = Field(default_factory=UISettings)
    editor: EditorSettings = Field(default_factory=EditorSettings)

    # Connection profiles
    connections: List[ConnectionProfile] = Field(default_factory=list)

    def __init__(self, **data):
        super().__init__(**data)
        self.ensure_config_directory()
        self.load_connections()

    def ensure_config_directory(self):
        """Ensure configuration directory exists"""
        self.config_dir.mkdir(parents=True, exist_ok=True)

    def save_connections(self):
        """Save connection profiles to file"""
        try:
            connections_data = [conn.model_dump() for conn in self.connections]
            with open(self.connections_file, "w") as f:
                json.dump(connections_data, f, indent=2, default=str)
        except Exception as e:
            print(f"Error saving connections: {e}")

    def load_connections(self):
        """Load connection profiles from file"""
        if not self.connections_file.exists():
            return

        try:
            with open(self.connections_file, "r") as f:
                connections_data = json.load(f)

            self.connections = [
                ConnectionProfile(**conn_data) for conn_data in connections_data
            ]
        except Exception as e:
            print(f"Error loading connections: {e}")
            self.connections = []

    def add_connection(self, connection: ConnectionProfile):
        """Add a new connection profile"""
        self.connections.append(connection)
        self.save_connections()

    def remove_connection(self, connection_name: str):
        """Remove a connection profile by name"""
        self.connections = [
            conn for conn in self.connections if conn.name != connection_name
        ]
        self.save_connections()

    def get_connection(self, name: str) -> Optional[ConnectionProfile]:
        """Get connection profile by name"""
        for conn in self.connections:
            if conn.name == name:
                return conn
        return None


# Global settings instance
settings = ApplicationSettings()
