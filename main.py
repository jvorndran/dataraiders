from SproutSocialClass import Sprout
import DATARAID as DR

connection = DR.connect_to_db()

sprout = Sprout()

owned_profiles = sprout.get_all_profiles()

for profile in owned_profiles:
    query = f"INSERT INTO OWNED_PROFILES (network_type, name, native_name, native_id) VALUES ('{profile['network_type']}', '{profile['name']}', '{owned_profiles['native_name']}', '{owned_profiles['native_id']}');"
    r = DR.query_table(connection, query)
    if r.success:
        print(f"Row inserted successfully for {profile}")
    else:
        print(f"Row insertion failed for {profile}")

DR.close_connection(connection)
