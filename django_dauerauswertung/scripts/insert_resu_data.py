import psycopg2
from pgcopy import CopyManager
# Connect to an existing database


def run():
    conn = psycopg2.connect("dbname=tsdb user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    for id in range(1, 6):
        simulate_query = """SELECT generate_series('2022-12-01 10:00:00.000'::timestamp, '2022-12-01 10:20:00.000'::timestamp, interval '1 sec') AS time,
                            %s as messpunkt_id,
                            random()*100 AS lafeq,
                            random()*100 AS lcfeq,
                            random()*100 AS lafmax
                            """
        data = (id,)
        cursor.execute(simulate_query, data)
        values = cursor.fetchall()

        # column names of the table you're inserting into
        cols = ['time', 'messpunkt_id', 'lafeq', 'lafmax', 'lcfeq']

        # create copy manager with the target table and insert
        mgr = CopyManager(conn, 'tsdb_resu', cols)
        mgr.copy(values)
    conn.commit()
    conn.close()