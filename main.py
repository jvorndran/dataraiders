from SproutSocialClass import Sprout
import DatabaseFunctions as DBF

INSERT_QUERY = ("INSERT INTO OWNED_PROFILES (network_type, name, native_name, native_id) "
                "VALUES (%s, %s, %s, %s);")


def insert_profiles_into_db(connection, profiles):
    for profile in profiles:
        params = (profile['network_type'], profile['name'], profile['native_name'], profile['native_id'])
        result = DBF.query_table(connection, INSERT_QUERY, params)
        if result.success:
            print(f"Row inserted successfully for {profile}")
        else:
            print(f"Row insertion failed for {profile}")


connection = DBF.connect_to_db()
sprout = Sprout()
owned_profiles = sprout.get_all_profiles()
insert_profiles_into_db(connection, owned_profiles)
DBF.close_connection(connection)
