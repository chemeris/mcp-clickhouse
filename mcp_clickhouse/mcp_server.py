from fastmcp import FastMCP
from typing import Sequence
from dotenv import load_dotenv
import clickhouse_connect
import os
import logging
import yaml


MCP_SERVER_NAME = "mcp-db-client"

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
    "psycopg2-binary",
    "python-dotenv",
    "uvicorn",
]

mcp = FastMCP(MCP_SERVER_NAME, dependencies=deps)

MCP_TOOL_PREFIX = config.get("mcp",{}).get("tool_prefix", "")
MCP_DB_DESCRIPTION = config.get("mcp",{}).get("db_description", "Database")

def create_db_client():
    db_type = config["db"].get("type", "clickhouse").lower()
    host = config["db"]["host"]
    port = config["db"]["port"]
    username = config["db"]["username"]
    password = config["db"]["password"]
    
    logger.info(f"Creating {db_type} client connection to {host}:{port} as {username}")
    
    if db_type == "clickhouse":
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
        )
    elif db_type == "postgres":
        import psycopg2
        return psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=config["db"].get("database", "postgres")
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def execute_query(client, query, readonly=False):
    db_type = config["db"].get("type", "clickhouse").lower()
    
    try:
        if db_type == "clickhouse":
            if readonly:
                result = client.query(query, settings={"readonly": 1})
                column_names = result.column_names
                rows = []
                for row in result.result_rows:
                    row_dict = {}
                    for i, col_name in enumerate(column_names):
                        row_dict[col_name] = row[i]
                    rows.append(row_dict)
                return rows
            else:
                return client.command(query)
        elif db_type == "postgres":
            cursor = client.cursor()
            cursor.execute(query)
            if readonly:
                column_names = [desc[0] for desc in cursor.description]
                rows = []
                for row in cursor.fetchall():
                    row_dict = {}
                    for i, col_name in enumerate(column_names):
                        row_dict[col_name] = row[i]
                    rows.append(row_dict)
                return rows
            else:
                return cursor.fetchall()
    except Exception as e:
        raise Exception(f"Query execution error: {str(e)}")


@mcp.tool(
    name=MCP_TOOL_PREFIX+"list_databases",
    description=f"List all databases in {MCP_DB_DESCRIPTION}")
def list_databases():
    try:
        logger.info("Listing all databases")
        client = create_db_client()
        db_type = config["db"].get("type", "clickhouse").lower()
        
        if db_type == "clickhouse":
            query = "SHOW DATABASES"
        elif db_type == "postgres":
            query = "SELECT datname FROM pg_database WHERE datistemplate = false"
        
        result = execute_query(client, query)
        logger.info(f"Found {len(result) if isinstance(result, list) else 1} databases")
        return result
    except Exception as err:
        logger.error(f"Error listing databases: {err}")
        return {"error": str(err)}


def get_table_info(client, database: str, table: str):
    try:
        logger.info(f"Getting schema info for table {database}.{table}")
        db_type = config["db"].get("type", "clickhouse").lower()
        
        if db_type == "clickhouse":
            schema_query = f"DESCRIBE TABLE {database}.`{table}`"
            create_table_query = f"SHOW CREATE TABLE {database}.`{table}`"
        elif db_type == "postgres":
            schema_query = f"""
                SELECT column_name as name, data_type as type, 
                       is_nullable as default_kind, column_default as default_expression
                FROM information_schema.columns 
                WHERE table_schema = '{database}' AND table_name = '{table}'
            """
            create_table_query = f"""
                SELECT pg_get_tabledef('{database}.{table}'::regclass::oid)
            """

        columns = execute_query(client, schema_query, readonly=True)
        create_table_result = execute_query(client, create_table_query, readonly=True)

        return {
            "database": database,
            "name": table,
            "columns": columns,
            "create_table_query": create_table_result[0] if db_type == "postgres" else create_table_result,
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
        client = create_db_client()
        db_type = config["db"].get("type", "clickhouse").lower()

        if db_type == "clickhouse":
            query = f"SHOW TABLES FROM {database}"
            if like:
                query += f" LIKE '{like}'"
        elif db_type == "postgres":
            query = f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{database}' 
                AND table_type = 'BASE TABLE'
            """
            if like:
                query += f" AND table_name LIKE '{like}'"

        result = execute_query(client, query, readonly=True)
        
        tables = []
        if db_type == "clickhouse":
            # Handle ClickHouse result format
            if isinstance(result, str):
                table_names = [t.strip() for t in result.split() if t.strip()]
            else:
                table_names = result
        else:
            # Handle Postgres result format
            table_names = [row['table_name'] for row in result]

        for table in table_names:
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
        client = create_db_client()
        rows = execute_query(client, query, readonly=True)
        logger.info(f"Query returned {len(rows)} rows")
        return rows
    except Exception as err:
        logger.error(f"Error executing query: {err}")
        return f"error running query: {err}"
