import logging
from pickle import TRUE
import sys
import psycopg2
from pgcopy import CopyManager
import pandas as pd
import numpy as np

from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime, timedelta
from messdaten_07_22 import read_resu_data_v1, read_terz_data_v1
from konfiguration import project_mannheim
from insert_messdaten import insert_resu, insert_mete, insert_terz
# Connect to an existing database
conn = psycopg2.connect("dbname=tsdb user=postgres password=password host=localhost port=5432")


# Open a cursor to perform database operations
# dbname=tsdb
# CREATE DATABASE tsdb;

# KUF-Server
token = "0ql08EobRW6A23j97jAkLyqNKIfQIKJS9_Wrw4mWIqBu795dl4cSfaykizl261h-QwY9BPDMUXbDCuFzlPQsfg=="
org = "kufi"
bucket = "dauerauswertung_immendingen"
bucket_id = "c6a3680b6746e4d8"
org_id = "ea7b98ca8acb0b14"

influx_url = "http://localhost:8086"

ISOFORMAT = "%Y-%m-%dT%H:%M:%SZ"
from constants import FORMAT


if __name__ == '__main__':
    logging.basicConfig(
    level=logging.DEBUG, format=FORMAT, handlers=[
        # logging.FileHandler("long_query.log"),
    logging.StreamHandler(sys.stdout)]
    )
    month = 7
    for i in range (0, 31):
        
        messpunkt_id = 7
        from_date = datetime(2022, month, 1, 0, 0, 0) + timedelta(days=i)
        to_date = datetime(2022, month, 2, 0, 0, 0) + timedelta(days=i)
        # read_resu_data_v1(from_date, to_date, "Immendingen MP 1")
        # read_terz_data_v1(datetime(2022, 6, 1, 6, 0, 0), datetime(2022, 6, 1, 22, 0, 0), "Immendingen MP 1")
        # print(read_mete_data_v1(from_date, to_date))
        # get_resu_all_mps(project_immendingen.name_in_db, project_immendingen.MPs[0:2] , from_date, to_date)
        # get_terz_all_mps(project_immendingen.name_in_db,project_immendingen.MPs[0:2], from_date, to_date)
        
        if True:
            df = read_terz_data_v1(from_date, to_date, project_mannheim.name_in_db, project_mannheim.MPs[0])
            print(df)
            insert_terz(df, messpunkt_id, True)
        if True:
            df = read_resu_data_v1(from_date, to_date, project_mannheim.name_in_db, project_mannheim.MPs[0])
            print(df)
            insert_resu(df, messpunkt_id, True)