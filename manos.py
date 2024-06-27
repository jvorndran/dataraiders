import csv
from Logger import logger
import DatabaseFunctions as DBF
import os
from datetime import datetime

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS SPROUT_EXTRACT_DATA (
    date DATETIME,
    network VARCHAR(255),
    message TEXT,
    followers INT,
    comments INT,
    shares INT,
    likes INT,
    sentiment VARCHAR(255),
    location VARCHAR(255),
    hashtags VARCHAR(255)
);
"""

INSERT_QUERY_TEMPLATE = """
INSERT INTO SPROUT_EXTRACT_DATA (date, network, message, followers, comments, shares, likes, sentiment, location, hashtags)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

def create_table(connection):
    response = DBF.query_database(connection, CREATE_TABLE_QUERY)
    if response['success']:
        logger.info("Table created successfully")
    else:
        logger.error(f"Failed to create table: {response['message']}")


def read_and_insert_data(connection, csv_file_path):
    if not os.path.exists(csv_file_path):
        logger.error(f"CSV file does not exist: {csv_file_path}")
        return
    with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)
        for row in csv_reader:

            date = datetime.strptime(row[0], "%d/%m/%Y %I:%M %p").strftime("%Y-%m-%d %H:%M:%S")
            network = row[1] if row[1] else None
            message = row[2].replace("\\", "\\\\") if row[2] else None
            followers = int(row[7]) if row[7] else 0
            comments = int(row[8]) if row[8] else 0
            shares = int(row[9]) if row[9] else 0
            likes = int(row[10]) if row[10] else 0
            sentiment = row[11] if row[11] else None
            location = row[12] if row[12] else None
            hashtags = row[13] if row[13] else None

            data = {
                "date": date,
                "network": network,
                "message": message,
                "followers": followers,
                "comments": comments,
                "shares": shares,
                "likes": likes,
                "sentiment": sentiment,
                "location": location,
                "hashtags": hashtags
            }

            insert_row(connection, data)

            
def insert_row(connection, row):
    response = DBF.query_database(connection, INSERT_QUERY_TEMPLATE, row)
    if response['success']:
        logger.info(f"Row inserted successfully {row}")
    else:
        logger.error(f"Failed to insert row: {row}, \n Error: {response['message']}")

def set_env_vars_locally():
    os.environ['MYSQL_USER'] = "Mustache"
    os.environ['MYSQL_PASSWORD'] = "]Ro4C)m8/dF1\(>]"
    os.environ['MYSQL_HOST'] = "10.119.4.52"
    os.environ['DB_NAME'] = "default"
    os.environ['SPROUT_ACCESS_TOKEN'] = ""

def main():
    set_env_vars_locally()
    connection = DBF.connect_to_db()
    create_table(connection)
    read_and_insert_data(connection=connection, csv_file_path='../sprout-extracts/sprout_extract.csv')



if __name__ == "__main__":
    main()
