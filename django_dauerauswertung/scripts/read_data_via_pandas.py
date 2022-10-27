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
    if False:
        resu_df = pds.read_sql(
            f"select * from \"tsdb_resu\" where messpunkt_id = {messpunkt_id} and time >= '{from_date}' and time < '{to_date}' ORDER BY TIME", dbConnection)
        print(resu_df)
    if False:
        multi_line_sql = f"""WITH old AS (SELECT id FROM tsdb_auswertungslauf WHERE zeitpunkt_im_beurteilungszeitraum = '{'2022-10-25 06:00:00+02:00'}' AND zuordnung_id = 2), 
        T1 AS (SELECT time AS t1time, dauer as t1dauer FROM tsdb_detected AS d JOIN old ON old.id = d.berechnet_von_id), T2 AS (SELECT time AS t2time, filter_id FROM tsdb_rejected AS d JOIN old ON old.id = d.berechnet_von_id),
        T3 AS (SELECT * FROM tsdb_resu where messpunkt_id = 7 and time >= '{'2022-10-25 06:00:00+02:00'}' and time < '{'2022-10-25 07:00:00+02:00'}' ORDER BY time),
        T4 AS ((SELECT * FROM (T3 LEFT JOIN T1 ON T3.time <= (t1time + (INTERVAL '1 sec' * t1dauer)) AND T3.time >= t1time )))
        SELECT * FROM (SELECT time, max(t1dauer) AS BLUB, min(t1dauer) AS BLA FROM T4 AS J1 LEFT JOIN T2 ON J1.time = t2time GROUP BY time) AS J2 WHERE J2.BLUB != J2.BLA ORDER BY time"""
        ml_df = pds.read_sql(multi_line_sql, dbConnection)
        print(ml_df)
    if True:
        io_id = 6
        for v_id in  [9, 8]:
            lr_df = pds.read_sql(
                f"select * from \"tsdb_lrpegel\" where immissionsort_id = {io_id} and time >= '{from_date}' and time < '{to_date}' and verursacht_id = {9} ORDER BY TIME", dbConnection)
            print(lr_df)
    if True:
        "SELECT count(*) FROM tsdb_lrpegel T1 JOIN (select * from tsdb_auswertungslauf where zeitpunkt_im_beurteilungszeitraum = '2022-10-25 06:00:00+02') T2 ON T1.berechnet_von_id = T2.id WHERE T1.immissionsort_id = 6 AND T1.verursacht_id = 9;"
    if False:
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
