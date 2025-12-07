# MCP SQL Server

A Model Context Protocol (MCP) server implementation for PostgreSQL databases, providing AI assistants with secure and structured access to PostgreSQL databases.

```bash
INFO:     Started server process [12345]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

## Features

- **Database Introspection**: List tables and explore database schemas
- **Table Schema Discovery**: Get detailed information about tables including columns, types, constraints, primary keys, and foreign keys
- **Safe Query Execution**: Execute read-only SELECT queries with built-in safety checks
- **Full Query Support**: Execute INSERT, UPDATE, DELETE, and SELECT queries with parameterized query support
- **Type Safety**: Built with Pydantic models for robust input validation
- **Comprehensive Logging**: Detailed logging for debugging and monitoring

## Tools Available

- `list_tables` - Lists all tables in a specified database schema.
- `get_table_schema` - Retrieves detailed schema information for a specific table.
- `execute_safe_query` - Executes read-only SELECT queries safely.
- `execute_query` - Executes any SQL query (SELECT, INSERT, UPDATE, DELETE) with optional parameterization.

## Configuration with MCP Clients

Add the server to your MCP client configuration (e.g., Claude Desktop):

## Testing

See [tests/README_TESTING.md](tests/README_TESTING.md) for detailed testing information.

## Security Considerations

- **Environment Variables**: Store database credentials in `.env` files or environment variables, never in code
- **Read-Only Access**: Consider using `execute_safe_query` for untrusted queries
- **Database Permissions**: Use database users with minimal necessary permissions
- **Parameterized Queries**: Always use the `params` argument for user-supplied values to prevent SQL injection
- **Network Security**: Restrict database access to trusted networks

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0) - see the [LICENSE](LICENSE) file for details.

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/atarkowska/mcp-sql-server).

## Related Projects

- [Model Context Protocol](https://modelcontextprotocol.io/)
- [FastMCP](https://github.com/jlowin/fastmcp)
- [asyncpg](https://github.com/MagicStack/asyncpg)
