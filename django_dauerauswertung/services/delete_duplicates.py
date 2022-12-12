    
from datetime import datetime, timedelta
import psycopg2    
conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5432/tsdb")
cursor = conn.cursor()


projekt_id = 1
q_tz = """SET TIME ZONE 'Europe/Rome'"""
cursor.execute(q_tz)

for d in range(1, 30+1):
    for i in [1,2,3,4,5,6]:
        messpunkt_id = i
        current_timestamp = datetime(2022, 10, d)
        next_day = current_timestamp + timedelta(days=1)
        q_resu = f"""DELETE FROM tsdb_resu WHERE id in (SELECT id FROM (SELECT time, min(id) AS id FROM tsdb_resu where time >= '{current_timestamp}' and time <= '{next_day}' and messpunkt_id = {messpunkt_id} GROUP BY time HAVING count(*) > 1) T) and time >= '{current_timestamp}' and time <= '{next_day}';"""
        q_terz = f"""DELETE FROM tsdb_terz WHERE id in (SELECT id FROM (SELECT time, min(id) AS id FROM tsdb_terz where time >= '{current_timestamp}' and time <= '{next_day}' and messpunkt_id = {messpunkt_id} GROUP BY time HAVING count(*) > 1) T) and time >= '{current_timestamp}' and time <= '{next_day}';"""

        print(q_resu)
        print(q_terz)
        
        cursor.execute(q_resu)
        cursor.execute(q_terz)
    conn.commit()

conn.close()