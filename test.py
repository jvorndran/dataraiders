import DATARAID as DR

CREATE_TABLE_QUERY = "CREATE TABLE OWNED_PROFILES (network_type VARCHAR(255), name VARCHAR(255), native_name VARCHAR(255), native_id VARCHAR(255));"
QUERY = "INSERT INTO OWNED_PROFILES (network_type, name, native_name, native_id) VALUES ('facebook', 'Test', 'Test', '123456789');"

connection = DR.connect_to_db()

r = DR.query_table(connection, CREATE_TABLE_QUERY)
if r.success:
    print("Table created successfully")
else:
    print("Table creation failed")
    exit(1)

r = DR.query_table(connection, QUERY)
if r.success:
    print("Row inserted successfully")
else:
    print("Row insertion failed")
    exit(1)

DR.close_connection(connection)

