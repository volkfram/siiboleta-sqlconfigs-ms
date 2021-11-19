#!/usr/bin/env python3

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_USER = "postgres"
DB_DATABASE = "siiboleta"
DB_PWD = "rcN3pK5aE7"
DB_HOST = "siiboleta-db.codqistsumyj.us-east-1.rds.amazonaws.com"
DB_PORT = "5432"


def generateDatabase(database_name):

    try:
        connection = psycopg2.connect(
            user=DB_USER, password=DB_PWD, host=DB_HOST, port=DB_PORT)

        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Obtain a DB Cursor
        cursor = connection.cursor()

        # Create table statement
        statement = "create database " + str(database_name) + ";"

        # Create a table in PostgreSQL database
        cursor.execute(statement)

    except (Exception, psycopg2.Error) as error:
        print("[E] Error while connecting to PostgreSQL : " + str(error))

    finally:
        if(connection):
            connection.close()
            print("[i] PostgreSQL connection is closed.")


if __name__ == "__main__":

    generateDatabase(DB_DATABASE)
