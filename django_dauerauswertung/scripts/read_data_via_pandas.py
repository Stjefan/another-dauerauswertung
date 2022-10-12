import psycopg2

import pandas as pds

from sqlalchemy import create_engine


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
