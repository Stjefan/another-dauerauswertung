import psycopg2
from pgcopy import CopyManager
from datetime import datetime, timedelta
# Connect to an existing database


def run():
    # http://localhost:8000/tsdb/evaluation/?messpunkt=1&time_after=2022-10-05T05%3A00%3A02&time_before=2022-10-05T05%3A00%3A06
    app_name = "tsdb"
    conn = psycopg2.connect("dbname=tsdb user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    time = datetime(2022, 12, 1, 0, 0, 0)
    q = f"INSERT INTO {app_name}_Detected (time, dauer, typ_id) VALUES ('{time}', 10, 1);"
    cursor.execute(q)
    q = f"INSERT INTO {app_name}_Rejected (time, filter_id) VALUES ('{time}', 1);"
    cursor.execute(q)
    
    q = f"INSERT INTO {app_name}_MaxPegel (time, immissionsort_id, pegel) VALUES ('{time}', 1, 44);"
    cursor.execute(q)
    q = f"INSERT INTO {app_name}_SchallleistungPegel (time, messpunkt_id, pegel) VALUES ('{time}', 1, 15);"
    cursor.execute(q)

    m = 2
    for io in range(1, 5):
        for i in range(1, 12+1):
            q = f"INSERT INTO {app_name}_LrPegel (time, immissionsort_id, verursacht_id, pegel) VALUES ('{time + timedelta(seconds=-1, minutes=5*i)}', {io}, 1, {m*i});"
            cursor.execute(q)

    conn.commit()
    conn.close()