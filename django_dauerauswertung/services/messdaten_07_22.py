

from datetime import datetime, timedelta
import sys

import pandas as pd
import requests
import numpy as np
import logging
import json

from konfiguration import project_immendingen, project_mannheim

from DTO import Messpunkt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from constants import frequencies
# from influx_api import (token, org, bucket, influx_url, ISOFORMAT)

# You can generate an API token from the "API Tokens Tab" in the UI
token = "0ql08EobRW6A23j97jAkLyqNKIfQIKJS9_Wrw4mWIqBu795dl4cSfaykizl261h-QwY9BPDMUXbDCuFzlPQsfg=="
org = "kufi"
bucket = "dauerauswertung_immendingen"



# read_resu_data(datetime(2022, 6, 2, 6, 0, 0), datetime(2022, 6, 2, 6, 15, 0))

def read_resu_data_v1(from_datetime: datetime, to_datetime: datetime, project_name: str, mp: Messpunkt):
    messpunkt_name = mp.bezeichnung_in_db
    r = requests.post(
        f"""http://localhost:8086/query?db=dauerauswertung_immendingen&q=Select * from messwerte_{project_name}_resu where time >= '{from_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}' AND time <= '{to_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}' AND messpunkt = '{messpunkt_name}'""",
        headers={"Authorization": f"Token {token}"})
    r.raise_for_status()
    parsed_r = r.json()["results"][0]["series"][0]

    data = np.array(parsed_r["values"])
    df = pd.DataFrame.from_records(data, columns=parsed_r["columns"])
    if False:
        df.rename(columns={"lafeq": f"R{mp.Id}_LAFeq", "lafmax": f"R{mp.Id}_LAFmax",
                "lcfeq": f"R{mp.Id}_LCFeq", "datetime": "Timestamp"}, inplace=True)
        df['Timestamp'] = pd.to_datetime(df['time'], infer_datetime_format=True)
        df = df.astype({f"R{mp.Id}_LAFeq": 'float',
                    f"R{mp.Id}_LAFmax": "float", f"R{mp.Id}_LCFeq": "float"})
        df_indexed = df.set_index("Timestamp")
        df_indexed.drop(["time", "messpunkt"], axis=1, inplace=True)
    else:
        df['Date/Time'] = pd.to_datetime(df['time'], infer_datetime_format=True)
        df = df[["lafeq", "lafmax", "lcfeq", "Date/Time"]]
        df = df.astype({f"lafeq": 'float',
                    f"lafmax": "float", f"lcfeq": "float"})
        df['Date/Time'] = df['Date/Time'].dt.tz_localize(None)
        df_indexed = df.set_index("Date/Time")

    return df_indexed





def read_terz_data_v1(from_datetime: datetime, to_datetime: datetime, project_name: str, mp: Messpunkt):
    messpunkt_name = mp.bezeichnung_in_db
    if False:
        with InfluxDBClient(url=influx_url, token=token, org=org, timeout=1000*600) as client:
            query_terz = f"""from(bucket: "dauerauswertung_immendingen")
                |> range(start: {from_datetime.strftime(ISOFORMAT)}, stop: {to_datetime.strftime(ISOFORMAT)})
                |> filter(fn: (r) => r["_measurement"] == "messwerte_{project_name}_terz" and r["messpunkt"] == "{messpunkt_name}")
                |> group(columns: ["time", "_field"])   |> keep(columns: ["_field", "_time", "_value"])
                """
            logging.info(f"Starting query: {query_terz}")
            df_terz = client.query_api().query_data_frame(query_terz, data_frame_index=["_time"])
            logging.info("Query has finished...")
            print(df_terz)
            # print(df_terz.groupby(by=["_field", "_time"]).first())
            print(df_terz["_value"])
    logging.info("Starting v1 query...")
    query = f"""http://localhost:8086/query?db=dauerauswertung_immendingen&q=Select * from messwerte_{project_name}_terz where time >= '{from_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}' AND time <= '{to_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}' AND messpunkt = '{messpunkt_name}'"""     
    r = requests.get(
        query,
        headers={"Authorization": f"Token {token}"})
    logging.info(query)
    r.raise_for_status()
    # logging.info(r.json()["results"])
    parsed_r = r.json()["results"][0]["series"][0]

    data = np.array(parsed_r["values"])
    terz_daten = pd.DataFrame.from_records(data, columns=parsed_r["columns"])

    terz_daten = terz_daten.astype(
        {"hz20": 'float',
         "hz25": 'float',
         "hz31_5": 'float',
         "hz40": 'float',
         "hz50": 'float',
         "hz63": 'float',
         "hz80": 'float',
         "hz100": 'float',
         "hz125": 'float',
         "hz160": 'float',
         "hz200": 'float',
         "hz250": 'float',
         "hz315": 'float',
         "hz400": 'float',
         "hz500": 'float',
         "hz630": 'float',
         "hz800": 'float',
         "hz1000": 'float',
         "hz1250": 'float',
         "hz1600": 'float',
         "hz2000": 'float',
         "hz2500": 'float',
         "hz3150": 'float',
         "hz4000": 'float',
         "hz5000": 'float',
         "hz6300": 'float',
         "hz8000": 'float',
         "hz10000": 'float',
         "hz12500": 'float',
         "hz16000": 'float',
         "hz20000": 'float'})
    if False:
        terz_daten.rename(columns={"hz20": f"T{mp.Id}_LZeq20", "hz25": f"T{mp.Id}_LZeq25", "hz31_5": f"T{mp.Id}_LZeq31_5", "hz40": f"T{mp.Id}_LZeq40", "hz50": f"T{mp.Id}_LZeq50", "hz63": f"T{mp.Id}_LZeq63", "hz80": f"T{mp.Id}_LZeq80", "hz100": f"T{mp.Id}_LZeq100", "hz125": f"T{mp.Id}_LZeq125", "hz160": f"T{mp.Id}_LZeq160",
                                "hz200": f"T{mp.Id}_LZeq200", "hz250": f"T{mp.Id}_LZeq250", "hz315": f"T{mp.Id}_LZeq315", "hz400": f"T{mp.Id}_LZeq400", "hz500": f"T{mp.Id}_LZeq500", "hz630": f"T{mp.Id}_LZeq630", "hz800": f"T{mp.Id}_LZeq800", "hz1000": f"T{mp.Id}_LZeq1000", "hz1250": f"T{mp.Id}_LZeq1250", "hz1600": f"T{mp.Id}_LZeq1600",
                                "hz2000": f"T{mp.Id}_LZeq2000", "hz2500": f"T{mp.Id}_LZeq2500", "hz3150": f"T{mp.Id}_LZeq3150", "hz4000": f"T{mp.Id}_LZeq4000", "hz5000": f"T{mp.Id}_LZeq5000", "hz6300": f"T{mp.Id}_LZeq6300", "hz8000": f"T{mp.Id}_LZeq8000", "hz10000": f"T{mp.Id}_LZeq10000", "hz12500": f"T{mp.Id}_LZeq12500", "hz16000": f"T{mp.Id}_LZeq16000",
                                "hz20000": f"T{mp.Id}_LZeq20000"}, inplace=True)
    if False:
        terz_daten['Timestamp'] = pd.to_datetime(
            terz_daten['time'], infer_datetime_format=True)
        terz_daten.drop(["time", "messpunkt"], axis=1, inplace=True)
        df_terz_single_mp_indexed_on_timestamp = terz_daten.set_index("Timestamp")
    else:
        terz_daten['Date/Time'] = pd.to_datetime(
            terz_daten['time'], infer_datetime_format=True)
        terz_daten = terz_daten[frequencies + ["Date/Time"]]
        terz_daten['Date/Time'] = terz_daten['Date/Time'].dt.tz_localize(None)
        

        df_terz_single_mp_indexed_on_timestamp = terz_daten.set_index("Date/Time")
    logging.info("v1 query has finished...")
    return df_terz_single_mp_indexed_on_timestamp


def get_terz_all_mps(project_name: str, mps, from_date, to_date) -> pd.DataFrame:
    df_mps = []
    for i in mps:
        df_mps.append(read_terz_data_v1(from_date, to_date, project_name, i))
    result = pd.concat(df_mps, axis=1, join="inner")
    return result


def get_resu_all_mps(project_name: str, mp_names, from_date, to_date) -> pd.DataFrame:
    df_mps = []
    for i in mp_names:
        df_mps.append(read_resu_data_v1(from_date, to_date, project_name, i))

    result = pd.concat(df_mps, axis=1, join="inner")

    return result


def read_mete_data_v1(project_name: str, from_datetime: datetime, to_datetime: datetime):
    r = requests.get(
        f"""http://localhost:8086/query?db=dauerauswertung_immendingen&q=Select * from messwerte_{project_name}_mete where time >= '{from_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}' AND time <= '{to_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")}'""",
        headers={"Authorization": f"Token {token}"})
    r.raise_for_status()
    parsed_r = r.json()["results"][0]["series"][0]

    data = np.array(parsed_r["values"])
    mete_daten = pd.DataFrame.from_records(data, columns=parsed_r["columns"])
    mete_daten = mete_daten.astype({
        "temperature": "float",
        "humidity": "float",
        "windspeed": "float",
        "pressure": "float",
        "winddirection": "float",
        "rain": "float"
    })
    mete_daten = mete_daten.rename(columns={"temperature": "Temperatur", "humidity": "Luftfeuchtigkeit", "windspeed": "MaxWindgeschwindigkeit", "pressure": "Druck", "winddirection": "Windrichtung", "rain": "Regen"})
    
    mete_daten['Timestamp'] = pd.to_datetime(
        mete_daten['time'], infer_datetime_format=True)
    mete_daten_indexed = mete_daten.set_index("Timestamp")
    mete_daten_indexed.drop(["time"], axis=1, inplace=True)

    return mete_daten_indexed


#datetime(2022, 6, 1, 10, 0, 0).isoformat()
# 2022-06-01T00:00:00Z
if __name__ == '__main__':
    from_date = datetime(2022, 4, 15, 6, 0, 0)
    to_date = datetime(2022, 4, 15, 22, 0, 0)
    # read_resu_data_v1(from_date, to_date, "Immendingen MP 1")
    # read_terz_data_v1(datetime(2022, 6, 1, 6, 0, 0), datetime(2022, 6, 1, 22, 0, 0), "Immendingen MP 1")
    # print(read_mete_data_v1(from_date, to_date))
    # get_resu_all_mps(project_immendingen.name_in_db, project_immendingen.MPs[0:2] , from_date, to_date)
    # get_terz_all_mps(project_immendingen.name_in_db,project_immendingen.MPs[0:2], from_date, to_date)
    logging.basicConfig(
    level=logging.DEBUG, handlers=[logging.FileHandler("long_query.log"),
    logging.StreamHandler(sys.stdout)]
    )
    read_terz_data_v1(from_date, to_date, project_mannheim.name_in_db, project_mannheim.MPs[0])

# do_dbrps_mapping()
