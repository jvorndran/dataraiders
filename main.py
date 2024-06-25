from SproutSocialClass import Sprout
import DatabaseFunctions as DBF
import os
from Logger import logger

INSERT_QUERY = ("INSERT INTO OWNED_PROFILES (network_type, name, native_name, native_id, deleted) "
                "VALUES (%s, %s, %s, %s, FALSE);")


def insert_sprout_profiles_into_db(connection, profiles):

    if not profiles:
        logger.error("No profiles to insert")
        return False

    if not connection.is_connected():
        logger.error("Connection to the database is closed")
        raise Exception("Connection to the database is closed")

    for profile in profiles:
        params = (profile['network_type'], profile['name'], profile['native_name'], profile['native_id'])
        result = DBF.query_database(connection, INSERT_QUERY, params)
        if result.success:
            logger.info(f"Row inserted successfully for {profile}")
        else:
            logger.error(f"Row insertion failed for {profile}")
            logger.error(f"Inserting profiles failed: {result.message}")
            raise Exception(result.message)

    logger.info("All profiles inserted successfully")
    return True


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
    try:
        check_env_vars()
        connection = DBF.connect_to_db()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return

    sprout = Sprout()

    try:
        owned_profiles = sprout.get_all_profiles()

        insert_sprout_profiles_into_db(connection, owned_profiles)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return
    finally:
        DBF.close_connection(connection)


if __name__ == "__main__":
    main()
