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


ts_connection_string = "dbname=tsdb user=postgres password=password host=localhost port=5432"
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


def insert_mete(df, messpunkt_id: int):
    
    cols = ['time', 'messpunkt_id'] + ["rain",
    "temperature",
    "windspeed",
    "pressure",
    "humidity",
    "winddirection"]

    records = []
    df_no_index = df.reset_index()
    df_no_index.insert(1, "messpunkt_id", messpunkt_id)
    records = df_no_index.to_numpy()
    print(records)
    # records = [(datetime.now(), 1, 20.0, 25.0, 21.0)]
    conn = psycopg2.connect(ts_connection_string)
    mgr = CopyManager(conn, 'tsdb_mete', cols)
    mgr.copy(records)
    conn.commit()

def insert_terz(df, messpunkt_id: int):
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
    cols = ['time', 'messpunkt_id'] + frequencies

    records = []
    df_no_index = df.reset_index()
    df_no_index.insert(1, "messpunkt_id", messpunkt_id)
    records = df_no_index.to_numpy()
    print(records)
    # records = [(datetime.now(), 1, 20.0, 25.0, 21.0)]
    conn = psycopg2.connect(ts_connection_string)
    mgr = CopyManager(conn, 'tsdb_terz', cols)
    mgr.copy(records)
    conn.commit()

def insert_resu(df, messpunkt_id: int):
    cols = ['time', 'messpunkt_id', 'lafeq', 'lafmax', 'lcfeq']

    records = []
    df_no_index = df.reset_index()
    df_no_index.insert(1, "messpunkt_id", messpunkt_id)
    records = df_no_index.to_numpy()
    # print(records)
    # records = [(df.index[0], 1, 20.0, 25.0, 21.0)]
    conn = psycopg2.connect(ts_connection_string)
    mgr = CopyManager(conn, 'tsdb_resu', cols)
    mgr.copy(records)
    conn.commit()

def delete_duplicates(messpunkt_id: int, time_from: datetime, time_to: datetime, table: str):
    conn = psycopg2.connect(ts_connection_string)
    cursor = conn.cursor()

    q = f"DELETE FROM {table} WHERE time >= '{time_from}' AND time <= '{time_to}' and messpunkt_id = {messpunkt_id}"
    cursor.execute(q)
    conn.commit()
    conn.close()

def push_blocks_2_database(project_name, mp_id: int, name_messpunkt, folder_name, zeitpunkt_str):

    df_mete = read_mete_file(f"{folder_name}{mp_id:05}_METE_{zeitpunkt_str}.csv")
    df_resu = read_resu_file(f"{folder_name}{mp_id:05}_RESU_{zeitpunkt_str}.csv")
    df_terz = read_terz_file(f"{folder_name}{mp_id:05}_TERZ_{zeitpunkt_str}.csv")
    delete_duplicates(mp_id, df_resu.index[0], df_resu.index[-1], "tsdb_resu")
    delete_duplicates(mp_id, df_terz.index[0], df_terz.index[-1], "tsdb_terz")

    insert_resu(df_resu, mp_id)
    insert_terz(df_terz, mp_id)
    if df_mete is not None:
        delete_duplicates(mp_id, df_mete.index[0], df_mete.index[-1], "tsdb_mete")
        insert_mete(df_mete, mp_id)

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
                    folder_name = "../dev/"
                    zeitpunkt_str = datum.strftime("%Y_%m_%d_%H")
                    push_blocks_2_database(project_name, 2, "test", folder_name, zeitpunkt_str)
                except Exception as ex:
                    logging.exception(f"Problem bei {h}")




