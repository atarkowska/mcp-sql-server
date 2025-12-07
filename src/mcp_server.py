# postgres_mcp_server_pydantic.py
"""
PostgreSQL MCP Server with Pydantic models for better type safety

Author: Ola Tarkowska
Copyright (C) 2025 Ola Tarkowska

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import logging
import os
import asyncpg
import json

from typing import Any, Optional, Dict, List
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field

from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("MCP PostgreSQL Database Server")

# Database connection configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", None)),
    "database": os.getenv("DB_DATABASE", None),
    "user": os.getenv("DB_USER", None),
    "password": os.getenv("DB_PASSWORD", None)
}


async def get_db_connection():
    """Create and return a database connection"""
    logger.debug(f"Connecting to database: {DB_CONFIG['database']} on {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    try:
        conn = await asyncpg.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        logger.debug("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


@mcp.tool()
async def list_tables(schema_name: str = "public") -> str:
    """
    List all tables in the specified schema.
    
    Args:
        schema_name: The database schema to list tables from (default: public)
    
    Returns:
        JSON string containing list of table names
    """
    logger.info(f"Listing tables in schema: {schema_name}")
    conn = await get_db_connection()
    try:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1 
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        logger.debug(f"Executing query: {query.strip()}")
        rows = await conn.fetch(query, schema_name)
        tables = [row['table_name'] for row in rows]
        logger.info(f"Found {len(tables)} tables in schema '{schema_name}'")
        logger.debug(f"Tables: {tables}")
        return json.dumps({
            "schema": schema_name,
            "tables": tables,
            "count": len(tables)
        }, indent=2)
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        raise
    finally:
        await conn.close()
        logger.debug("Database connection closed")


@mcp.tool()
async def get_table_schema(table_name: str, schema_name: str = "public") -> str:
    """
    Get the schema/structure of a specific table including columns, types, and constraints.
    
    Args:
        table_name: Name of the table to get schema for
        schema_name: The database schema (default: public)
    
    Returns:
        JSON string containing table schema information
    """
    logger.info(f"Getting table schema for: {schema_name}.{table_name}")
    conn = await get_db_connection()
    try:
        # Get column information
        logger.debug("Fetching column information")
        column_query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = $1 
            AND table_name = $2
            ORDER BY ordinal_position;
        """
        columns = await conn.fetch(column_query, schema_name, table_name)
        
        if not columns:
            logger.warning(f"Table '{table_name}' not found in schema '{schema_name}'")
            return json.dumps({
                "error": f"Table '{table_name}' not found in schema '{schema_name}'",
                "status": "failed"
            }, indent=2)
        
        logger.debug(f"Found {len(columns)} columns")
        # Get primary key information
        logger.debug("Fetching primary key information")
        pk_query = """
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid 
                AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::regclass
            AND i.indisprimary;
        """
        pks = await conn.fetch(pk_query, f"{schema_name}.{table_name}")
        primary_keys = [pk['column_name'] for pk in pks]
        logger.debug(f"Primary keys: {primary_keys}")
        
        # Get foreign key information
        logger.debug("Fetching foreign key information")
        fk_query = """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2;
        """
        fks = await conn.fetch(fk_query, schema_name, table_name)
        
        # Format the response
        column_info = []
        for col in columns:
            col_dict = {
                "name": col['column_name'],
                "type": col['data_type'],
                "nullable": col['is_nullable'] == 'YES',
                "default": col['column_default'],
                "is_primary_key": col['column_name'] in primary_keys
            }
            if col['character_maximum_length']:
                col_dict['max_length'] = col['character_maximum_length']
            column_info.append(col_dict)
        
        foreign_keys = [
            {
                "column": fk['column_name'],
                "references_table": fk['foreign_table_name'],
                "references_column": fk['foreign_column_name']
            }
            for fk in fks
        ]
        
        logger.info(f"Successfully retrieved schema for {schema_name}.{table_name}: {len(column_info)} columns, {len(primary_keys)} PKs, {len(foreign_keys)} FKs")
        return json.dumps({
            "schema": schema_name,
            "table": table_name,
            "columns": column_info,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys
        }, indent=2)
    except Exception as e:
        logger.error(f"Error getting table schema: {e}")
        return json.dumps({
            "error": str(e),
            "status": "failed"
        }, indent=2)
    finally:
        await conn.close()
        logger.debug("Database connection closed")


@mcp.tool()
async def execute_query(query: str, params: Optional[list] = None) -> str:
    """
    Execute a SQL query and return the results.
    
    Args:
        query: The SQL query to execute (SELECT, INSERT, UPDATE, DELETE)
        params: Optional list of parameters for parameterized queries
    
    Returns:
        JSON string containing query results or affected row count
    """
    logger.info(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
    if params:
        logger.debug(f"Query parameters: {params}")
    conn = await get_db_connection()
    try:
        # Determine query type
        query_upper = query.strip().upper()
        
        if query_upper.startswith('SELECT'):
            # For SELECT queries, return results
            if params:
                rows = await conn.fetch(query, *params)
            else:
                rows = await conn.fetch(query)
            
            # Convert to list of dicts
            results = [dict(row) for row in rows]
            logger.info(f"SELECT query returned {len(results)} rows")
            
            return json.dumps({
                "type": "SELECT",
                "row_count": len(results),
                "results": results
            }, indent=2, default=str)
        else:
            # For INSERT, UPDATE, DELETE, return affected rows
            if params:
                status = await conn.execute(query, *params)
            else:
                status = await conn.execute(query)
            
            # Extract affected row count from status
            affected = status.split()[-1] if status else "0"
            logger.info(f"{query_upper.split()[0]} query affected {affected} rows")
            
            return json.dumps({
                "type": query_upper.split()[0],
                "affected_rows": affected,
                "status": "success"
            }, indent=2)
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return json.dumps({
            "error": str(e),
            "status": "failed"
        }, indent=2)
    finally:
        await conn.close()
        logger.debug("Database connection closed")


@mcp.tool()
async def execute_safe_query(query: str) -> str:
    """
    Execute a read-only SELECT query safely.
    This tool only allows SELECT queries for safety.
    
    Args:
        query: The SELECT query to execute
    
    Returns:
        JSON string containing query results
    """
    logger.info(f"Executing safe query: {query[:100]}{'...' if len(query) > 100 else ''}")
    # Validate that it's a SELECT query
    if not query.strip().upper().startswith('SELECT'):
        logger.warning(f"Rejected non-SELECT query in safe mode: {query[:50]}")
        return json.dumps({
            "error": "Only SELECT queries are allowed with this tool",
            "status": "failed"
        }, indent=2)
    
    return await execute_query(query)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='PostgreSQL MCP Server')
    parser.add_argument('--port', type=int, default=8000, help='Port to run the server on')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--transport', type=str, default='streamable-http', help='Transport')
    args = parser.parse_args()
    
    logger.info(f"Starting MCP PostgreSQL Server on {args.host}:{args.port} with transport={args.transport}")
    logger.info(f"Database config: {DB_CONFIG['database']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
    
    mcp.run(transport=args.transport, host=args.host, port=args.port)