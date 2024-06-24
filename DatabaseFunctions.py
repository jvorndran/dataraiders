# Script that tests connecting to the cloud sql db and inserting a row into a table
from mysql.connector import Error, connect
import os


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
            print(f"Connected to MySQL Server version {db_info}")
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print(f"Connected to database: {record}")

        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        raise Exception(f"Error: {e}")


def close_connection(connection):
    if connection.is_connected():
        connection.close()
        connection.cursor().close()
        print("MySQL connection is closed")


def query_table(connection, query, params=None):
    cursor = connection.cursor()
    response = {}
    try:
        cursor.execute(query, params)
        connection.commit()
        response['message'] = "Query Successful"
        response['success'] = True
    except Error as e:
        response['message'] = str(e)
        response['success'] = False
        raise Exception(f"Error: {e}")

    return response
