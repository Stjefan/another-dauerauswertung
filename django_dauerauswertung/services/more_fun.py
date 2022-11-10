import logging
from datetime import datetime, timedelta
import psycopg2



import logging
import sys

import logging
import logging.config

from datetime import datetime
from auswertung_service import get_project_via_rest
from monatsbericht import Monatsbericht, MonatsuebersichtAnImmissionsort, MonatsuebersichtAnImmissionsortV2


def get_auswertung_gesamt(current_time: datetime):
    m = Monatsbericht()

def get_auswertung_an_immissionsort(immissionsort_id: int, current_time: datetime):
    conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5432/tsdb")
    cursor = conn.cursor()
    # current_time = datetime.now() + timedelta(hours=-6)

    m = MonatsuebersichtAnImmissionsortV2()

    
    q_tz = """SET TIME ZONE 'Europe/Rome'"""
    cursor.execute(q_tz)

    after_time =   datetime(current_time.year, current_time.month, 1, 0, 0, 0)
    before_time = datetime(current_time.year, current_time.month, 31, 0, 0, 0)

    q_day = f"""
            SELECT Date, pegel FROM (
                SELECT time::date AS Date, time, pegel, ROW_NUMBER() OVER (
                PARTITION BY time::date
	            ORDER BY pegel DESC, time
            ) as rank FROM tsdb_lrpegel lr 
            WHERE time >= '{after_time}' AND time <= '{before_time}' AND (time::time >= '06:00' OR time::time <= '22:00') AND immissionsort_id = {immissionsort_id}) T1
            WHERE T1.rank = 1;
        """
    cursor.execute(q_day)
    results = cursor.fetchall()
    for r in results:
        m.lr_tag[r[0]] = r[1]

    q_night = f"""
            SELECT Date, Stunde, pegel FROM (
                SELECT time::date AS Date, time AS Stunde, pegel, ROW_NUMBER() OVER (
                    PARTITION BY time::date
                    ORDER BY pegel DESC, time
                ) as rank FROM tsdb_lrpegel lr 
                WHERE time >= '{after_time}' AND time <= '{before_time}' AND (time::time <= '06:00' OR time::time >= '22:00') AND immissionsort_id = {immissionsort_id}) T1
            WHERE T1.rank = 1;
        """
    cursor.execute(q_night)
    results = cursor.fetchall()
    for r in results:
        m.lr_max_nacht[r[0]] = (r[2], r[1])

    q_max_pegel_night = f"""
            SELECT Date, pegel, extract('hour' from time) AS Stunde FROM (
                SELECT time::date AS Date, time, pegel, ROW_NUMBER() OVER (
                    PARTITION BY time::date
                    ORDER BY pegel DESC, time
                ) as rank FROM tsdb_maxpegel 
                WHERE time >= '{after_time}' AND time <= '{before_time}' AND (time::time <= '06:00' OR time::time >= '22:00') AND immissionsort_id = {immissionsort_id}) T1
            WHERE T1.rank = 1 ORDER BY Date;
        """

    cursor.execute(q_max_pegel_night)
    results = cursor.fetchall()
    for r in results:
        m.lauteste_stunde_nacht[r[0]] = (r[2], r[1])

    q_max_pegel_day = f"""
            SELECT * FROM (
                SELECT time::date, time, pegel, ROW_NUMBER() OVER (
                PARTITION BY time::date
	            ORDER BY pegel DESC, time
            ) as rank FROM tsdb_maxpegel
            WHERE time >= '{after_time}' AND time <= '{before_time}' AND (time::time >= '06:00' OR time::time <= '22:00') AND immissionsort_id = {immissionsort_id}) T1
            WHERE T1.rank = 1;
        """
    cursor.execute(q_max_pegel_day)
    results = cursor.fetchall()
    for r in results:
        m.lauteste_stunde_tag[r[0]] = (r[2], r[1])

    q = f"""SELECT generate_series('{after_time}'::date, '{before_time}'::date, interval '1 day')"""

    

    conn.close()
    return m
    

p = get_project_via_rest("mannheim")
for io in p.IOs:
    get_auswertung_an_immissionsort(io.id_in_db, datetime(2022, 10, 3))