import snowflake.connector


def get_snowflake_conn():
    """
    Establishes and returns a connection to the Snowflake data warehouse.

    Connection details:
    - User: <YOUR_USERNAME>
    - Account: ACCOUNT
    - Warehouse: COMPUTE_WH
    - Database: banking
    - Schema: analytics

    Returns:
        snowflake.connector.connection.SnowflakeConnection:
            An active connection object to the Snowflake instance.

    Raises:
        snowflake.connector.errors.Error: If the connection could not be established.

    Usage example:
        conn = get_snowflake_conn()
        # Use `conn` to execute queries and interact with Snowflake
    """
    return snowflake.connector.connect(
        user="<YOUR_USERNAME>",
        password="<YOUR_PASSWORD>",
        account="ACCOUNT",
        warehouse="COMPUTE_WH",
        database="banking",
        schema="analytics"
    )
