"""
Unit tests for PostgreSQL MCP Server
"""
import pytest
import json
import asyncpg
from unittest.mock import AsyncMock, MagicMock, patch
from mcp_server import (
    get_db_connection,
    list_tables,
    get_table_schema,
    execute_query,
    execute_safe_query,
    DB_CONFIG
)


@pytest.fixture
def mock_db_config():
    """Mock database configuration"""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password"
    }


@pytest.fixture
def mock_connection():
    """Mock asyncpg connection"""
    conn = AsyncMock(spec=asyncpg.Connection)
    conn.fetch = AsyncMock()
    conn.execute = AsyncMock()
    conn.close = AsyncMock()
    return conn


class TestGetDbConnection:
    """Tests for get_db_connection function"""
    
    @pytest.mark.asyncio
    @patch('mcp_server.asyncpg.connect')
    async def test_successful_connection(self, mock_connect):
        """Test successful database connection"""
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        
        result = await get_db_connection()
        
        assert result == mock_conn
        mock_connect.assert_called_once_with(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
    
    @pytest.mark.asyncio
    @patch('mcp_server.asyncpg.connect')
    async def test_connection_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = asyncpg.PostgresError("Connection failed")
        
        with pytest.raises(asyncpg.PostgresError):
            await get_db_connection()


class TestListTables:
    """Tests for list_tables function"""
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_list_tables_success(self, mock_get_conn, mock_connection):
        """Test successful table listing"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = [
            {'table_name': 'users'},
            {'table_name': 'products'},
            {'table_name': 'orders'}
        ]
        
        result = await list_tables(schema_name="public")
        result_data = json.loads(result)
        
        assert result_data["schema"] == "public"
        assert result_data["count"] == 3
        assert "users" in result_data["tables"]
        assert "products" in result_data["tables"]
        assert "orders" in result_data["tables"]
        mock_connection.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_list_tables_empty(self, mock_get_conn, mock_connection):
        """Test listing tables in empty schema"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = []
        
        result = await list_tables(schema_name="empty_schema")
        result_data = json.loads(result)
        
        assert result_data["schema"] == "empty_schema"
        assert result_data["count"] == 0
        assert result_data["tables"] == []
        mock_connection.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_list_tables_custom_schema(self, mock_get_conn, mock_connection):
        """Test listing tables in custom schema"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = [
            {'table_name': 'custom_table'}
        ]
        
        result = await list_tables(schema_name="custom_schema")
        result_data = json.loads(result)
        
        assert result_data["schema"] == "custom_schema"
        assert result_data["count"] == 1


class TestGetTableSchema:
    """Tests for get_table_schema function"""
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_get_table_schema_success(self, mock_get_conn, mock_connection):
        """Test successful table schema retrieval"""
        mock_get_conn.return_value = mock_connection
        
        # Mock column information
        mock_connection.fetch.side_effect = [
            # Columns
            [
                {
                    'column_name': 'id',
                    'data_type': 'integer',
                    'character_maximum_length': None,
                    'is_nullable': 'NO',
                    'column_default': 'nextval(...)'
                },
                {
                    'column_name': 'name',
                    'data_type': 'character varying',
                    'character_maximum_length': 255,
                    'is_nullable': 'YES',
                    'column_default': None
                }
            ],
            # Primary keys
            [{'column_name': 'id'}],
            # Foreign keys
            []
        ]
        
        result = await get_table_schema(table_name="users", schema_name="public")
        result_data = json.loads(result)
        
        assert result_data["schema"] == "public"
        assert result_data["table"] == "users"
        assert len(result_data["columns"]) == 2
        assert result_data["columns"][0]["name"] == "id"
        assert result_data["columns"][0]["is_primary_key"] is True
        assert result_data["columns"][1]["name"] == "name"
        assert result_data["columns"][1]["max_length"] == 255
        assert result_data["primary_keys"] == ["id"]
        mock_connection.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_get_table_schema_not_found(self, mock_get_conn, mock_connection):
        """Test table schema for non-existent table"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = []
        
        result = await get_table_schema(table_name="nonexistent", schema_name="public")
        result_data = json.loads(result)
        
        assert "error" in result_data
        assert result_data["status"] == "failed"
        assert "not found" in result_data["error"]
        mock_connection.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_get_table_schema_with_foreign_keys(self, mock_get_conn, mock_connection):
        """Test table schema with foreign key relationships"""
        mock_get_conn.return_value = mock_connection
        
        mock_connection.fetch.side_effect = [
            # Columns
            [
                {
                    'column_name': 'order_id',
                    'data_type': 'integer',
                    'character_maximum_length': None,
                    'is_nullable': 'NO',
                    'column_default': None
                },
                {
                    'column_name': 'user_id',
                    'data_type': 'integer',
                    'character_maximum_length': None,
                    'is_nullable': 'NO',
                    'column_default': None
                }
            ],
            # Primary keys
            [{'column_name': 'order_id'}],
            # Foreign keys
            [
                {
                    'column_name': 'user_id',
                    'foreign_table_name': 'users',
                    'foreign_column_name': 'id'
                }
            ]
        ]
        
        result = await get_table_schema(table_name="orders", schema_name="public")
        result_data = json.loads(result)
        
        assert len(result_data["foreign_keys"]) == 1
        assert result_data["foreign_keys"][0]["column"] == "user_id"
        assert result_data["foreign_keys"][0]["references_table"] == "users"
        assert result_data["foreign_keys"][0]["references_column"] == "id"


class TestExecuteQuery:
    """Tests for execute_query function"""
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_select_query(self, mock_get_conn, mock_connection):
        """Test executing SELECT query"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        result = await execute_query("SELECT * FROM users")
        result_data = json.loads(result)
        
        assert result_data["type"] == "SELECT"
        assert result_data["row_count"] == 2
        assert len(result_data["results"]) == 2
        assert result_data["results"][0]["name"] == "Alice"
        mock_connection.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_select_query_with_params(self, mock_get_conn, mock_connection):
        """Test executing SELECT query with parameters"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = [
            {'id': 1, 'name': 'Alice'}
        ]
        
        result = await execute_query(
            "SELECT * FROM users WHERE id = $1",
            params=[1]
        )
        result_data = json.loads(result)
        
        assert result_data["type"] == "SELECT"
        assert result_data["row_count"] == 1
        mock_connection.fetch.assert_called_once_with(
            "SELECT * FROM users WHERE id = $1",
            1
        )
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_insert_query(self, mock_get_conn, mock_connection):
        """Test executing INSERT query"""
        mock_get_conn.return_value = mock_connection
        mock_connection.execute.return_value = "INSERT 0 1"
        
        result = await execute_query(
            "INSERT INTO users (name) VALUES ($1)",
            params=['Charlie']
        )
        result_data = json.loads(result)
        
        assert result_data["type"] == "INSERT"
        assert result_data["status"] == "success"
        assert result_data["affected_rows"] == "1"
        mock_connection.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_update_query(self, mock_get_conn, mock_connection):
        """Test executing UPDATE query"""
        mock_get_conn.return_value = mock_connection
        mock_connection.execute.return_value = "UPDATE 2"
        
        result = await execute_query("UPDATE users SET active = true")
        result_data = json.loads(result)
        
        assert result_data["type"] == "UPDATE"
        assert result_data["status"] == "success"
        assert result_data["affected_rows"] == "2"
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_delete_query(self, mock_get_conn, mock_connection):
        """Test executing DELETE query"""
        mock_get_conn.return_value = mock_connection
        mock_connection.execute.return_value = "DELETE 3"
        
        result = await execute_query("DELETE FROM users WHERE active = false")
        result_data = json.loads(result)
        
        assert result_data["type"] == "DELETE"
        assert result_data["status"] == "success"
        assert result_data["affected_rows"] == "3"
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_query_error(self, mock_get_conn, mock_connection):
        """Test query execution error"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.side_effect = asyncpg.PostgresError("Syntax error")
        
        result = await execute_query("SELECT * FROM invalid_syntax")
        result_data = json.loads(result)
        
        assert "error" in result_data
        assert result_data["status"] == "failed"
        assert "Syntax error" in result_data["error"]
        mock_connection.close.assert_called_once()


class TestExecuteSafeQuery:
    """Tests for execute_safe_query function"""
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_safe_query_select(self, mock_get_conn, mock_connection):
        """Test safe execution of SELECT query"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = [
            {'id': 1, 'name': 'Alice'}
        ]
        
        result = await execute_safe_query("SELECT * FROM users")
        result_data = json.loads(result)
        
        assert result_data["type"] == "SELECT"
        assert result_data["row_count"] == 1
    
    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_insert(self):
        """Test that safe query rejects INSERT"""
        result = await execute_safe_query("INSERT INTO users (name) VALUES ('test')")
        result_data = json.loads(result)
        
        assert "error" in result_data
        assert result_data["status"] == "failed"
        assert "Only SELECT queries" in result_data["error"]
    
    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_update(self):
        """Test that safe query rejects UPDATE"""
        result = await execute_safe_query("UPDATE users SET name = 'test'")
        result_data = json.loads(result)
        
        assert "error" in result_data
        assert result_data["status"] == "failed"
    
    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_delete(self):
        """Test that safe query rejects DELETE"""
        result = await execute_safe_query("DELETE FROM users")
        result_data = json.loads(result)
        
        assert "error" in result_data
        assert result_data["status"] == "failed"
    
    @pytest.mark.asyncio
    async def test_execute_safe_query_rejects_drop(self):
        """Test that safe query rejects DROP"""
        result = await execute_safe_query("DROP TABLE users")
        result_data = json.loads(result)
        
        assert "error" in result_data
        assert result_data["status"] == "failed"
    
    @pytest.mark.asyncio
    @patch('mcp_server.get_db_connection')
    async def test_execute_safe_query_with_whitespace(self, mock_get_conn, mock_connection):
        """Test safe query with leading whitespace"""
        mock_get_conn.return_value = mock_connection
        mock_connection.fetch.return_value = []
        
        result = await execute_safe_query("   SELECT * FROM users")
        result_data = json.loads(result)
        
        assert result_data["type"] == "SELECT"
