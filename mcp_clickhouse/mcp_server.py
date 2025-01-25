from fastmcp import FastMCP
from typing import Sequence
from dotenv import load_dotenv
import clickhouse_connect
import os
import logging
import yaml


MCP_SERVER_NAME = "mcp-clickhouse"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(MCP_SERVER_NAME)

load_dotenv()

CONFIG_FILE = os.getenv("CONFIG_FILE", "config.yml")

config = {}
with open(CONFIG_FILE, "r") as file:
    config = yaml.safe_load(file)

deps = [
    "clickhouse-connect",
    "python-dotenv",
    "uvicorn",
]

mcp = FastMCP(MCP_SERVER_NAME, dependencies=deps)

MCP_TOOL_PREFIX = config.get("mcp",{}).get("tool_prefix", "")
MCP_DB_DESCRIPTION = config.get("mcp",{}).get("db_description", "ClickHouse")

@mcp.tool(
    name=MCP_TOOL_PREFIX+"list_databases",
    description=f"List all databases in {MCP_DB_DESCRIPTION}")
def list_databases():
    try:
        logger.info("Listing all databases")
        client = create_clickhouse_client()
        result = client.command("SHOW DATABASES")
        logger.info(f"Found {len(result) if isinstance(result, list) else 1} databases")
        return result
    except Exception as err:
        logger.error(f"Error listing databases: {err}")
        return {"error": str(err)}


def get_table_info(client, database: str, table: str):
    try:
        logger.info(f"Getting schema info for table {database}.{table}")
        schema_query = f"DESCRIBE TABLE {database}.`{table}`"
        schema_result = client.query(schema_query)

        columns = []
        column_names = schema_result.column_names
        for row in schema_result.result_rows:
            column_dict = {}
            for i, col_name in enumerate(column_names):
                column_dict[col_name] = row[i]
            columns.append(column_dict)

        create_table_query = f"SHOW CREATE TABLE {database}.`{table}`"
        create_table_result = client.command(create_table_query)

        return {
            "database": database,
            "name": table,
            "columns": columns,
            "create_table_query": create_table_result,
        }
    except Exception as err:
        logger.error(f"Error getting table info: {err}")
        return {"error": str(err)}

@mcp.tool(
    name=MCP_TOOL_PREFIX+"list_tables",
    description=f"""List all tables in {MCP_DB_DESCRIPTION} for a given database name and their schema.
Also returns a list of columns and the CREATE TABLE query for each table.
Specify 'like' parameter to filter tables by name.
Omit the 'like' parameter or pass empty string to list all tables in the database.""")
def list_tables(database: str, like: str = None):
    try:
        logger.info(f"Listing tables in database '{database}'")
        client = create_clickhouse_client()
        query = f"SHOW TABLES FROM {database}"
        if like:
            query += f" LIKE '{like}'"
        result = client.command(query)

        tables = []
        if isinstance(result, str):
            # Single table result
            for table in (t.strip() for t in result.split()):
                if table:
                    tables.append(get_table_info(client, database, table))
        elif isinstance(result, Sequence):
            # Multiple table results
            for table in result:
                tables.append(get_table_info(client, database, table))

        logger.info(f"Found {len(tables)} tables")
        return tables
    except Exception as err:
        logger.error(f"Error listing tables: {err}")
        return {"error": str(err)}

@mcp.tool(
    name=MCP_TOOL_PREFIX+"run_select_query",
    description=f"Run a SELECT query on the {MCP_DB_DESCRIPTION}"
)
def run_select_query(query: str):
    try:
        logger.info(f"Executing SELECT query: {query}")
        client = create_clickhouse_client()
        res = client.query(query, settings={"readonly": 1})
        column_names = res.column_names
        rows = []
        for row in res.result_rows:
            row_dict = {}
            for i, col_name in enumerate(column_names):
                row_dict[col_name] = row[i]
            rows.append(row_dict)
        logger.info(f"Query returned {len(rows)} rows")
        return rows
    except Exception as err:
        logger.error(f"Error executing query: {err}")
        return f"error running query: {err}"


def create_clickhouse_client():
    host = config["db"]["host"]
    port = config["db"]["port"]
    username = config["db"]["username"]
    logger.info(f"Creating ClickHouse client connection to {host}:{port} as {username}")
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=config["db"]["password"],
    )
