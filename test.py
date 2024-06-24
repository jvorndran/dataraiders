import DatabaseFunctions as DBF

CREATE_TABLE_QUERY = "CREATE TABLE OWNED_PROFILES (network_type VARCHAR(255), name VARCHAR(255), native_name VARCHAR(255), native_id VARCHAR(255), deleted BOOLEAN DEFAULT FALSE);"
INSERT_QUERY = "INSERT INTO OWNED_PROFILES (network_type, name, native_name, native_id, deleted) VALUES ('facebook', 'Test', 'Test', '123456789', FALSE);"


def create_table(connection, query):
    r = DBF.query_database(connection, query)
    if r['success']:
        print("Table created successfully")
        return True
    else:
        raise Exception(r['message'])


def insert_into_table(connection, query):
    r = DBF.query_database(connection, query)
    if r['success']:
        print("Row inserted successfully")
        return True
    else:
        raise Exception(r['message'])


def main():
    connection = DBF.connect_to_db()
    try:
        create_table(connection, CREATE_TABLE_QUERY)
        insert_into_table(connection, INSERT_QUERY)
    except Exception as e:
        print(e)
        exit(1)
    finally:
        DBF.close_connection(connection)


if __name__ == "__main__":
    main()
