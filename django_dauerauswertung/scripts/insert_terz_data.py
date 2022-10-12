
frequencies = ['hz20', 'hz25', 'hz31_5', 'hz40', 'hz50', 'hz63', 'hz80', 'hz100', 'hz125', 'hz160', 'hz200', 'hz250',
                            'hz315', 'hz400', 'hz500', 'hz630', 'hz800', 'hz1000', 'hz1250', 'hz1600', 'hz2000', 'hz2500', 'hz3150',
                            'hz4000',
                            'hz5000',
                            'hz6300',
                            'hz8000',
                            'hz10000',
                            'hz12500',
                            'hz16000',
                            'hz20000']

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