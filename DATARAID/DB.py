# Script that tests connecting to the cloud sql db and inserting a row into a table
import mysql.connector
from mysql.connector import Error
import os


# Set up connection to the cloud sql db
def connect_to_db():
    try:
        connection = mysql.connector.connect(
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


def close_connection(connection):
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")


def query_table(connection, query):
    cursor = connection.cursor()
    response = {}
    try:
        response['message'] = cursor.execute(query)
        connection.commit()
        response['success'] = True
    except Error:
        response['error'] = True

    return response
