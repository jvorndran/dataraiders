import DatabaseFunctions as DBF
from Logger import logger
import os

CREATE_TABLE_QUERY = "CREATE TABLE OWNED_PROFILES (network_type VARCHAR(255), name VARCHAR(255), native_name VARCHAR(255), native_id VARCHAR(255), deleted BOOLEAN DEFAULT FALSE);"
INSERT_QUERY = "INSERT INTO OWNED_PROFILES (network_type, name, native_name, native_id, deleted) VALUES ('facebook', 'Test', 'Test', '123456789', FALSE);"


def create_table(connection, query):
    r = DBF.query_database(connection, query)
    if r['success']:
        logger.info("Table created successfully")
        return True
    else:
        logger.error(r['message'])
        raise Exception(r['message'])


def insert_into_table(connection, query):
    r = DBF.query_database(connection, query)
    if r['success']:
        logger.info("Row inserted successfully")
        return True
    else:
        logger.error(r['message'])
        raise Exception(r['message'])


def check_env_vars():
    """
    Check if the required environment variables are set.

    :return: None
    """
    required_env_vars = ['MYSQL_USER', 'MYSQL_PASSWORD', 'MYSQL_HOST', 'DB_NAME', 'SPROUT_ACCESS_TOKEN']
    for var in required_env_vars:
        if var not in os.environ:
            logger.error(f"Environment variable {var} is not set")
            raise Exception(f"Environment variable {var} is not set")


def main():
    connection = DBF.connect_to_db()
    try:
        check_env_vars()
        create_table(connection, CREATE_TABLE_QUERY)
        insert_into_table(connection, INSERT_QUERY)
        logger.info("All operations completed successfully")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        exit(1)
    finally:
        DBF.close_connection(connection)


if __name__ == "__main__":
    main()
