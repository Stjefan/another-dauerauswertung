import psycopg2
# Connect to an existing database


def run():
    conn = psycopg2.connect("dbname=tsdb user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    q = """
    Select * from tsdb_terz AS TERZ JOIN tsdb_resu AS RESU ON TERZ.time = Resu.time Where TERZ.time = '2022-08-01 10:00:00' and TERZ.time <= '2022-08-01 10:00:10' and TERZ.messpunkt_id = 2
    """
    q = """
    Select * from tsdb_resu AS RESU LEFT JOIN tsdb_nichtverwertbarerzeitpunkt AS INVALID ON RESU.time = INVALID.time WHERE RESU.time >= '2022-08-01 10:00:00' and RESU.time <= '2022-08-01 10:00:10'
    AND RESU.messpunkt_id = 3"""
    q = """
    SELECT COUNT(*) FROM (Select RESU.*, ERKENNUNG.time AS T from tsdb_resu AS RESU LEFT JOIN tsdb_erkennung AS ERKENNUNG ON
    RESU.time >= ERKENNUNG.time AND RESU.time <= (ERKENNUNG.time + (INTERVAL '1 sec' * ERKENNUNG.dauer)) 
    WHERE RESU.time >= '2022-08-01 10:00:00' AND RESU.time <= '2022-08-01 10:15:00'
    AND RESU.messpunkt_id = 3) AS JOINED_RESU WHERE T is not null"""
    cursor.execute(q)
    print(cursor.fetchall())