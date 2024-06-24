from SproutSocialClass import Sprout
import DatabaseFunctions as DBF

INSERT_QUERY = ("INSERT INTO OWNED_PROFILES (network_type, name, native_name, native_id, deleted) "
                "VALUES (%s, %s, %s, %s, FALSE);")


def insert_sprout_profiles_into_db(connection, profiles):

    if not profiles:
        print("No profiles to insert")
        return False

    if not connection.is_connected():
        raise Exception("Connection to the database is closed")

    for profile in profiles:
        params = (profile['network_type'], profile['name'], profile['native_name'], profile['native_id'])
        result = DBF.query_database(connection, INSERT_QUERY, params)
        if result.success:
            print(f"Row inserted successfully for {profile}")
        else:
            print(f"Row insertion failed for {profile}")
            raise Exception(result.message)

    return True


def main():
    connection = DBF.connect_to_db()
    sprout = Sprout()
    owned_profiles = sprout.get_all_profiles()

    profiles_inserted_successfully = insert_sprout_profiles_into_db(connection, owned_profiles)
    if profiles_inserted_successfully:
        print("All profiles inserted successfully")
    else:
        print("Failed to insert all profiles")

    DBF.close_connection(connection)


if __name__ == "__main__":
    main()
