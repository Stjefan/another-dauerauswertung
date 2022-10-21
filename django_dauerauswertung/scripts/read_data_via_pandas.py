import psycopg2

import pandas as pds

from sqlalchemy import create_engine
from datetime import datetime

def read_data():
    alchemyEngine = create_engine(
        'postgresql://postgres:password@127.0.0.1:5432/tsdb')


    # Connect to PostgreSQL server

    dbConnection = alchemyEngine.connect()


    # Read data from PostgreSQL database table and load into a DataFrame instance
    from_date = datetime(2022,10,5,6,0,0)
    to_date = datetime(2022,10,5,22,0,0)
    messpunkt_id = 7
    resu_df = pds.read_sql(
        f"select * from \"tsdb_resu\" where messpunkt_id = {messpunkt_id} and time >= '{from_date}' and time < '{to_date}' ORDER BY TIME", dbConnection)

    print(resu_df)

    terz_df = pds.read_sql(
        f"select * from \"tsdb_terz\" where messpunkt_id = {messpunkt_id} and time >= '{from_date}' and time < '{to_date}' ORDER BY TIME", dbConnection)

    print(terz_df)

    mete_df = pds.read_sql(
        f"select * from \"tsdb_mete\" where time >= '{from_date}' and time < '{to_date}' ORDER BY TIME", dbConnection)

    print(mete_df)

    # Close the database connection

    dbConnection.close()



if __name__ == "__main__":
    read_data()
    if False:
    # Create an engine instance

        alchemyEngine = create_engine(
            'postgresql://postgres:password@127.0.0.1:5432/tsdb')


        # Connect to PostgreSQL server

        dbConnection = alchemyEngine.connect()


        # Read data from PostgreSQL database table and load into a DataFrame instance

        dataFrame = pds.read_sql(
            "select * from \"tsdb_resu\" where messpunkt_id = 4 and time >= '2022-10-08 08:00:00'", dbConnection)



        pds.set_option('display.expand_frame_repr', False)


        # Print the DataFrame

        print(dataFrame)

        dataFrame = pds.read_sql(
            "select * from \"tsdb_terz\" where messpunkt_id = 4 and time >= '2022-10-06 08:00:00'", dbConnection)

        print(dataFrame)

        # Close the database connection

        dbConnection.close()
