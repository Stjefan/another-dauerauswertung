
from services.dauerauswertung_constants import frequencies

import psycopg2
from pgcopy import CopyManager
# Connect to an existing database


def run():
    conn = psycopg2.connect("dbname=tsdb user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    for id in range(1, 6):
        simulate_query = """SELECT generate_series('2022-08-01 10:00:00.0'::timestamp, '2022-08-01 10:10:00.0'::timestamp, interval '1 sec') AS time,
                            """

        for f in frequencies:
            simulate_query += f"random()*100 AS {f},"

        simulate_query += "%s as messpunkt_id"

        data = (id,)
        cursor.execute(simulate_query, data)
        values = cursor.fetchall()

        print(simulate_query)

        # column names of the table you're inserting into
        cols = ['time']
        for f in frequencies:
            cols.append(f)
        cols.append('messpunkt_id')
        # create copy manager with the target table and insert
        mgr = CopyManager(conn, 'tsdb_terz', cols)
        mgr.copy(values)
    conn.commit()
    conn.close()