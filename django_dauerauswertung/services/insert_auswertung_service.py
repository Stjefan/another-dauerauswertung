import math
from random import random
import psycopg2
from pgcopy import CopyManager
from datetime import datetime, timedelta
from DTO import Auswertungsergebnis, Detected, LrPegel, Ergebnisse, DTO_LrPegel
import requests
import logging
import sys
from psycopg2.extras import execute_values


from constants import get_start_end_beurteilungszeitraum_from_datetime

# Connect to an existing database

app_name = "tsdb"

def delete_old_data_via_psycopg2(cursor, id_old_auswertungslauf, from_date, to_date):
    for tbl in ["lrpegel", "rejected", "detected", "schallleistungpegel", "maxpegel"]:
        q = f"""
                DELETE FROM {app_name}_{tbl} WHERE time >= '{from_date}' and time <= '{to_date}' and berechnet_von_id = {id_old_auswertungslauf};
                """
        cursor.execute(q)
    cursor.execute(f"""DELETE FROM {app_name}_Auswertungslauf WHERE id = {id_old_auswertungslauf}""")
    

def get_id_old_auswertungslauf(cursor, from_date, to_date):
    q = f"""SELECT id FROM {app_name}_Auswertungslauf WHERE zeitpunkt_im_beurteilungszeitraum  >= '{from_date}' and zeitpunkt_im_beurteilungszeitraum <= '{to_date}'"""
    cursor.execute(q)
    
    result = cursor.fetchone()

    print(result)
    return result[0] if result else 0

def insert_new_auswertungslauf(cursor, from_date, to_date, ergebnis: Ergebnisse):

    time = from_date
    q = f"""
        INSERT INTO {app_name}_Auswertungslauf 
        (zeitpunkt_im_beurteilungszeitraum, zeitpunkt_durchfuehrung, verhandene_messwerte, verwertebare_messwerte, in_berechnung_gewertete_messwerte, zuordnung_id) 
        VALUES 
        ('{datetime.now()}', '{ergebnis.zeitpunkt_im_beurteilungszeitraum}', {ergebnis.verhandene_messwerte}, {ergebnis.verwertebare_messwerte}, {ergebnis.in_berechnung_gewertete_messwerte}, {ergebnis.zuordnung}) 
        RETURNING id;
        """
    cursor.execute(q)
    new_row_id = cursor.fetchone()
    lr_arr = [[i.time.isoformat(), i.pegel, i.verursacht, i.immissionsort, new_row_id] for i in ergebnis.lrpegel_set]
    rejected_set = [[i.time.isoformat(), i.grund, 1, new_row_id] for i in ergebnis.rejected_set]
    detected_set = []
    maxpegel_set = [[i.time.isoformat(), i.pegel, i.id_immissionsort, new_row_id] for i in ergebnis.maxpegel_set]
    schallleistungspegel_set = [[i.time.isoformat(), i.pegel, i.id_messpunkt, new_row_id] for i in ergebnis.schallleistungspegel_set]


    execute_values(cursor, """INSERT INTO tsdb_lrpegel (time, pegel, immissionsort_id, verursacht_id, berechnet_von_id) VALUES %s""", lr_arr)
    execute_values(cursor, """INSERT INTO tsdb_rejected (time, filter_id, messpunkt_id, berechnet_von_id) VALUES %s""", rejected_set)
    execute_values(cursor, """INSERT INTO tsdb_detected ( time, dauer, messpunkt_id, typ_id, berechnet_von_id) VALUES %s""", detected_set)
    execute_values(cursor, """INSERT INTO tsdb_maxpegel ( time, pegel, immissionsort_id, berechnet_von_id) VALUES  %s""", maxpegel_set)
    execute_values(cursor, """INSERT INTO tsdb_schallleistungpegel (time, pegel, messpunkt_id, berechnet_von_id) VALUES %s""", schallleistungspegel_set)


def insert_auswertung_via_psycopg2(time, ergebnis: Ergebnisse):
    conn = psycopg2.connect(
        "dbname=tsdb user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    
    from_date, to_date = get_start_end_beurteilungszeitraum_from_datetime(time)

    delete_old_data_via_psycopg2(cursor, get_id_old_auswertungslauf(cursor, from_date, to_date), from_date, to_date)
    insert_new_auswertungslauf(cursor, from_date, to_date, ergebnis)
    conn.commit()
    


if __name__ == "__main__":
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
    level=logging.INFO, format=FORMAT, handlers=[
        #logging.FileHandler("eval.log"),
        logging.StreamHandler(sys.stdout)
        ]
    )
    url = "http://localhost:8000/tsdb/auswertungslauf/"
    time = datetime(2021, 6, 1, 0, 0, 0)
    insert_auswertung_via_psycopg2(time)
    