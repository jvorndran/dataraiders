# Script that tests connecting to the cloud sql db and inserting a row into a table
from mysql.connector import Error, connect
import os
from Logger import logger


# Set up connection to the cloud sql db
def connect_to_db():
    try:
        connection = connect(
            user=os.environ.get('MYSQL_USER'),
            password=os.environ.get('MYSQL_PASSWORD'),
            host=os.environ.get('MYSQL_HOST'),
            database=os.environ.get('DB_NAME')
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            logger.info(f"Connected to MySQL Server version {db_info}")
            with connection.cursor() as cursor:
                cursor.execute("select database();")
                record = cursor.fetchone()
                logger.info(f"Connected to database: {record}")

        return connection
    except Error as e:
        logger.error(f"The error '{e}' occurred")
        raise Exception(f"Error: {e}")


def close_connection(connection):
    """
    Closes the MySQL connection.

    :param connection: the MySQL connection object
    :return: None
    """
    if connection.is_connected():
        connection.close()
        connection.cursor().close()
        logger.info("MySQL connection is closed")


def query_database(connection, query, params=None):
    """
    :param connection: The database connection object.
    :type connection: mysql.connector.connection.MySQLConnection
    :param query: The SQL query to be executed.
    :type query: str
    :param params: Optional. The parameters to be passed to the query.

    :return: A dictionary containing the response of the query.
             The dictionary has two keys:
             - 'message': A string message indicating the success or failure of the query.
             - 'success': A boolean value indicating whether the query was successful (True) or not (False).
    """
    response = {}
    with connection.cursor() as cursor:
        try:
            cursor.execute(query, params)
            connection.commit()
            response['message'] = "Query Successful"
            response['success'] = True
        except Error as e:
            response['message'] = str(e)
            response['success'] = False
            connection.rollback()
            logger.warning(f"The Query {query} failed")

    return response


def batch_insert(connection, table_name, columns, values_list):
    """
    Executes a batch insert operation in a database table.

    :param connection: The database connection object.
    :type connection: mysql.connector.connection.MySQLConnection
    :param table_name: The name of the table to insert data into.
    :type table_name: str
    :param columns: The list of column names to insert data into.
    :type columns: list of str
    :param values_list: The list of value lists to be inserted into the table.
    :type values_list: list of lists
    :return: A string message indicating the success or failure of the batch insert operation.
    :rtype: str
    """
    with connection.cursor() as cursor:
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        try:
            cursor.executemany(query, values_list)
            connection.commit()
            logger.info("Batch insert successful")
            return
        except Error as e:
            connection.rollback()
            logger.warning(f"Batch Insert failed: {e}")
            return


def update_row(connection, table, updates, condition):
    """
    :param connection: the database connection object
    :type connection: mysql.connector.connection.MySQLConnection
    :param table: the name of the table to update
    :type table: str
    :param updates: a dictionary containing the column names as keys and values to update
    :type updates: dict
    :param condition: the condition to use in the WHERE clause of the update query
    :type condition: str
    :return: a string message indicating the success or failure of the update
    :rtype: str

    The update_row function updates rows in a database table based on the given parameters. It takes in a database
    connection object, the table name, a dictionary of updates, and a condition for the update query. The updates
    dictionary should contain the column names as keys and the values to update as values. The condition should be a
    valid SQL condition that will be used in the WHERE clause of the update query.

    If the update is successful, the function will commit the changes to the database and print "Update successful". If
    an error occurs, the function will print the error message and rollback the changes.

    Example usage:
    connection = create_connection()
    table = "customers"
    updates = {"first_name": "John", "last_name": "Doe"}
    condition = "id = 1"
    update_row(connection, table, updates, condition)
    """
    with connection.cursor() as cursor:
        updates = ', '.join(f'{col} = %s' for col in updates.keys())
        query = f"UPDATE {table} SET {updates} WHERE {condition}"
        try:
            cursor.execute(query, tuple(updates.values()))
            connection.commit()
            logger.info("Update successful")
        except Error as e:
            connection.rollback()
            logger.warning(f"Update failed: {e}")


def execute_script(connection, script):
    """
    Execute a script on a database connection.

    :param connection: The database connection object.
    :type connection: mysql.connector.connection.MySQLConnection
    :param script: The script to be executed.
    :type script: str
    :return: A string message indicating the success or failure of the script execution.
    :rtype str

    """
    with connection.cursor() as cursor:
        try:
            for statement in script.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            connection.commit()
            logger.info("Script executed successfully")
        except Error as e:
            connection.rollback()
            logger.warning(f"Script execution failed: {e}")

