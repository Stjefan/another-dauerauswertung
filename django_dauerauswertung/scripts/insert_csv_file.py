import psycopg2
from pgcopy import CopyManager
from datetime import datetime, timedelta
# Connect to an existing database
from calendar import monthrange
from datetime import datetime
from io import StringIO
import logging
import re
import sys
import pandas as pd

rename_dict_mete = {
    "Temperatur": "temperature",
    "Druck": "pressure",
    "Luftfeuchtigkeit": "humidity",
    "Regen": "rain",
    "Windrichtung": "winddirection",
    "Max Wind-Geschwindigkeit": "windspeed"

}

rename_dict_resu = {
    "LAFeq": "lafeq",
    "LAFmax": "lafmax",
    "LCFeq": "lcfeq"
}

rename_dict_terz = {
    "LZeq 20 Hz":  "hz20",
    "LZeq 25 Hz":  "hz25",
    "LZeq 31,5 Hz":  "hz31_5",
    "LZeq 40 Hz":  "hz40",
    "LZeq 50 Hz":  "hz50",
    "LZeq 63 Hz":  "hz63",
    "LZeq 80 Hz":  "hz80",
    "LZeq 100 Hz":  "hz100",
    "LZeq 125 Hz":  "hz125",
    "LZeq 160 Hz":  "hz160",
    "LZeq 200 Hz":  "hz200",
    "LZeq 250 Hz":  "hz250",
    "LZeq 315 Hz":  "hz315",
    "LZeq 400 Hz":  "hz400",
    "LZeq 500 Hz":  "hz500",
    "LZeq 630 Hz":  "hz630",
    "LZeq 800 Hz":  "hz800",
    "LZeq 1000 Hz":  "hz1000",
    "LZeq 1250 Hz":  "hz1250",
    "LZeq 1600 Hz":  "hz1600",
    "LZeq 2000 Hz":  "hz2000",
    "LZeq 2500 Hz":  "hz2500",
    "LZeq 3150 Hz":  "hz3150",
    "LZeq 4000 Hz":  "hz4000",
    "LZeq 5000 Hz":  "hz5000",
    "LZeq 6300 Hz":  "hz6300",
    "LZeq 8000 Hz":  "hz8000",
    "LZeq 10000 Hz":  "hz10000",
    "LZeq 12500 Hz":  "hz12500",
    "LZeq 16000 Hz":  "hz16000",
    "LZeq 20000 Hz":  "hz20000"
    }

custom_date_parser = lambda x: datetime.strptime(x, "%d.%m.%Y %H:%M:%S")

def read_mete_file(filepath: str):
    logging.info(filepath)
    try:
        data_file_without_mete = open(filepath)
        content = data_file_without_mete.read()


        data_block_without_trailing_colon = re.sub(';\n', '\n', content)

        content_messdaten = StringIO(data_block_without_trailing_colon)
        df = pd.read_csv(content_messdaten, sep = ";", parse_dates=[0], decimal=",", date_parser=custom_date_parser)
        df = df.drop(["Mitl. Windgeschwindigkeit"], axis=1)

        df = df.rename(columns=rename_dict_mete)
        df.set_index("Date/Time", inplace=True)
        return df
    except FileNotFoundError as ex:
        logging.warning(ex)
        return None

def read_resu_file(filepath: str):
    logging.info(filepath)
    data_file_without_mete = open(filepath)
    content = data_file_without_mete.read()


    data_block_without_trailing_colon = re.sub(';\n', '\n', content)

    content_messdaten = StringIO(data_block_without_trailing_colon)
    df = pd.read_csv(content_messdaten, sep = ";", parse_dates=[0], decimal=",", date_parser=custom_date_parser)
    df = df.drop(["LCFmax", "LZFeq", "LZFmax"], axis=1)
    
    df = df.rename(columns=rename_dict_resu)
    df.set_index("Date/Time", inplace=True)
    return df

def read_terz_file(filepath: str):
    logging.info(filepath)
    data_file_without_mete = open(filepath)
    content = data_file_without_mete.read()


    data_block_without_trailing_colon = re.sub(';\n', '\n', content)

    content_messdaten = StringIO(data_block_without_trailing_colon)
    df = pd.read_csv(content_messdaten, sep = ";", parse_dates=[0], decimal=",", date_parser=custom_date_parser)
    
    df = df.rename(columns=rename_dict_terz)
    df.set_index("Date/Time", inplace=True)
    return df

def insert_resu(df):
    cols = ['time', 'messpunkt_id', 'lafeq', 'lafmax', 'lcfeq']

    records = df.to_records()
    conn = psycopg2.connect("dbname=tsdb user=postgres password=password host=localhost port=5432")
    mgr = CopyManager(conn, 'tsdb_resu', cols)
    mgr.copy(records)

# don't forget to commit!
    conn.commit()

def push_blocks_2_database(project_name, mp_id: int, name_messpunkt, folder_name, zeitpunkt_str):

    df_mete = read_mete_file(f"{folder_name}{mp_id:05}_METE_{zeitpunkt_str}.csv")
    df_resu = read_resu_file(f"{folder_name}{mp_id:05}_RESU_{zeitpunkt_str}.csv")
    df_terz = read_terz_file(f"{folder_name}{mp_id:05}_TERZ_{zeitpunkt_str}.csv")
    conn = psycopg2.connect("postgres://postgres:password@localhost:5432/tsdb")
    print(df_resu)
    insert_resu(df_resu)

    # print(df_resu)

    # data_points = transform_messdaten_2_influx_point_sequence(project_name, name_messpunkt, df_resu, df_terz, df_mete)

    logging.info("Start pushing...")
    # push_data(data_points)
    logging.info("Push succeded...")
    

def run():
    # http://localhost:8000/tsdb/evaluation/?messpunkt=1&time_after=2022-10-05T05%3A00%3A02&time_before=2022-10-05T05%3A00%3A06
    project_name = "sindelfingen"
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("../logs/block_file_push.log"),
        logging.StreamHandler(sys.stdout)]
    )
    # 5 is still missing
    for month in [6
        ]:
        year = 2021
        number_days_in_month = monthrange(year, month)[1]

        for d in range(1, 1 + 1):
            for h in range(0,1):
                try:
                    datum : datetime = datetime(year, month, d, h, 0, 0)
                    folder_name = "C:\CSV Zielordner\MB Sifi MP2 - Bau 46" + "/" + datum.strftime("%Y-%m/%d/%H/") # mp.OrdnerMessdaten + "/"+ datum.strftime("%Y-%m/%d/%H/")
                    zeitpunkt_str = datum.strftime("%Y_%m_%d_%H")
                    push_blocks_2_database(project_name, 2, "test", folder_name, zeitpunkt_str)
                except Exception as ex:
                    logging.info(f"Problem bei {h}")
                    logging.error(ex)




