
from io import BytesIO
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging

from DTO import Messpunkt
from messdaten_service import terzfrequenzen
from datetime import datetime, timedelta
"""Date/Time;LZeq 20 Hz;LZeq 25 Hz;LZeq 31,5 Hz;LZeq 40 Hz;LZeq 50 Hz;LZeq 63 Hz;LZeq 80 Hz;LZeq 100 Hz;LZeq 125 Hz;LZeq 160 Hz;LZeq 200 Hz;LZeq 250 Hz;LZeq 315 Hz;LZeq 400 Hz;LZeq 500 Hz;LZeq 630 Hz;LZeq 800 Hz;LZeq 1000 Hz;LZeq 1250 Hz;LZeq 1600 Hz;LZeq 2000 Hz;LZeq 2500 Hz;LZeq 3150 Hz;LZeq 4000 Hz;LZeq 5000 Hz;LZeq 6300 Hz;LZeq 8000 Hz;LZeq 10000 Hz;LZeq 12500 Hz;LZeq 16000 Hz;LZeq 20000 Hz"""
"""Date/Time;Temperatur;Druck;Luftfeuchtigkeit;Regen;Mitl. Windgeschwindigkeit;Windrichtung;Max Wind-Geschwindigkeit"""

def create_block_csv_resu(messpunkt, from_date, to_date):
    alchemyEngine = create_engine('postgresql://postgres:password@127.0.0.1:5432/tsdb')
    dbConnection = alchemyEngine.connect()
            

    q = f"select lafeq, lcfeq, lafmax, time from \"tsdb_resu\" where messpunkt_id = {messpunkt.id_in_db} and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME"
    print("Query", q)
    resu_df = pd.read_sql(q, dbConnection)


    data_dict = {
        "lafeq": f"LAFeq",
        "lafmax": f"_LAFmax",
        "lcfeq": f"LCFeq",
        "time": "Date/Time"
    }
    resu_df.rename(columns=data_dict, inplace=True)

    resu_df["LCFmax"] = np.nan
    resu_df["LZFeq"] = np.nan
    resu_df["LZFmax"] = np.nan
    # print(resu_df["Timestamp"].iloc[0].tzinfo)
    resu_df['Date/Time'] = resu_df['Date/Time'].dt.tz_convert('Europe/Berlin')
    # cet = pytz.timezone('CET').utcoffset()
    # resu_df['Timestamp'] = resu_df['Timestamp'] + cet
    resu_df['Date/Time'] = resu_df['Date/Time'].dt.tz_localize(None)
    resu_df.set_index("Date/Time", inplace=True)


    dti3 = pd.date_range(from_date, to_date, freq="1s", name="Timestamp")
    df_full_interval = pd.DataFrame(index=dti3)

    resu_df = df_full_interval.join(resu_df, how='left')

    myfile=BytesIO()

    resu_df.to_csv(myfile, decimal=",", sep=";", date_format="%d.%m.%Y %H:%M:%S")
    return myfile



def create_block_csv_terz(messpunkt, from_date, to_date):
    alchemyEngine = create_engine('postgresql://postgres:password@127.0.0.1:5432/tsdb')
    dbConnection = alchemyEngine.connect()
    terz_prefix = "LZeq"
    available_cols_terz = []
    cols_terz = []
    rename_cols = {}

    cols_in_db = ["time"]
    for k in terzfrequenzen:
        available_cols_terz.append(terz_prefix + k)
        cols_in_db.append("hz" + k)
        rename_cols["hz" + k] = f"LZeq {k.replace('_', ',')} Hz"
        
        
    t_arr = [f"""T{messpunkt.Id}"""]  # "T2", "T3", "T4", "T5", "T6"]
    
    for i in available_cols_terz:
        for j in t_arr:
            pass
            # cols_terz.append(f"{j}_{i}")
        
    q = f"select {','.join(cols_in_db)} from \"tsdb_terz\" where messpunkt_id = {messpunkt.id_in_db} and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME"
    logging.info(q)
    terz_df = pd.read_sql(q, dbConnection)



    terz_df.rename(columns={**rename_cols, "time": "Date/Time"}, inplace=True)
    
    # print(terz_df)
    terz_df["Date/Time"] = terz_df["Date/Time"].dt.tz_convert('Europe/Berlin')
    terz_df["Date/Time"] = terz_df["Date/Time"].dt.tz_localize(None)
    # cet = pytz.timezone('CET').utcoffset()
    # resu_df['Timestamp'] = resu_df['Timestamp'] + cet

    terz_df.set_index("Date/Time", inplace=True)

    dti3 = pd.date_range(from_date, to_date, freq="1s", name="Timestamp")
    df_full_interval = pd.DataFrame(index=dti3)

    terz_df = df_full_interval.join(terz_df, how='left')

    myfile=BytesIO()

    terz_df.to_csv(myfile, decimal=",", sep=";", date_format="%d.%m.%Y %H:%M:%S")
    return myfile

def create_block_csv_mete(messpunkt, from_date, to_date):
    
    alchemyEngine = create_engine('postgresql://postgres:password@127.0.0.1:5432/tsdb')
    dbConnection = alchemyEngine.connect()
            

    rename_dict = {
                "time": 'Date/Time',
                "winddirection": "Windrichtung",
                "rain":"Regen",
                "windspeed": "Max Wind-Geschwindigkeit",
                "temperature": "Temperatur",
                "pressure": "Druck",
                "humidity": "Luftfeuchtigkeit",
                
            }
    mete_df = pd.read_sql(f"select time, rain, temperature, windspeed, pressure, humidity, winddirection from \"tsdb_mete\" where messpunkt_id = {messpunkt.id_in_db} and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME", dbConnection)

    mete_df.rename(columns=rename_dict, inplace=True)
    mete_df["Mitl. Windgeschwindigkeit"] = mete_df["Max Wind-Geschwindigkeit"]
    mete_df['Date/Time'] = mete_df['Date/Time'].dt.tz_convert('Europe/Berlin')
    mete_df['Date/Time'] = mete_df['Date/Time'].dt.tz_localize(None)
    mete_df.set_index("Date/Time", inplace=True)

    dti3 = pd.date_range(from_date, to_date, freq="1s", name="Timestamp")
    df_full_interval = pd.DataFrame(index=dti3)

    mete_df = df_full_interval.join(mete_df, how='left')
    myfile=BytesIO()

    my_csv_mete = mete_df.to_csv(myfile, decimal=",", sep=";", date_format="%d.%m.%Y %H:%M:%S")
    return myfile


def get_lrpegel_csv(from_date, to_date):
    alchemyEngine = create_engine('postgresql://postgres:password@127.0.0.1:5432/tsdb')
    dbConnection = alchemyEngine.connect()

    modified_from_date = datetime(from_date.year, from_date.month, from_date.day) + timedelta(seconds=899)
    
    lr_df = pd.read_sql(f"""
        SELECT T1.time, 0 as id, pegel, id_external FROM 
        (SELECT * FROM tsdb_lrpegel WHERE time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' AND Mod(extract("minute" from time) + 1, 15) = 0  ORDER BY TIME) T1 JOIN
        (SELECT * FROM tsdb_LaermursacheAnImmissionsorten) T2 ON T1.verursacht_id = T2.id and T2.name = 'Gesamt' 
        JOIN tsdb_immissionsort T3 ON T3.id = T1.immissionsort_id AND t3.projekt_id = 1
        """, dbConnection)
    print(lr_df)
    #Timestamp;id;Beurteilungspegel;IdImmissionsort
    lr_df.rename(columns={
        "time": "Timestamp",
        "pegel": "Beurteilungspegel",
        "id_external": "IdImmissionsort"
    }, inplace=True)
    lr_df["Timestamp"] = lr_df['Timestamp'].dt.tz_convert('Europe/Berlin')
    lr_df['Timestamp'] = lr_df['Timestamp'].dt.tz_localize(None)
    lr_df.set_index("Timestamp", inplace=True)

    dti3 = pd.date_range(modified_from_date, to_date, freq="900s", name="Timestamp")
    df_full_interval = pd.DataFrame(index=dti3)

    lr_df = df_full_interval.join(lr_df, how='left')
    # Bemerkung: Bei fehlenden Daten wird nur eine Reihe hinzugefügt anstatt eine für jede Messreihe

    myfile=BytesIO()
    lr_df.to_csv(myfile, decimal=",", sep=";", date_format="%d.%m.%Y %H:%M:%S")
    return myfile

if __name__ == "__main__":
    mp = Messpunkt("2", id_in_db=2)
    tp = datetime.now()
    from_date = datetime(tp.year, tp.month, tp.day) + timedelta(days=-9) #datetime(2022, 10, 20)
    to_date = from_date + timedelta(days=7)
    get_lrpegel_csv(from_date, to_date)

    for i in range(0,23):
        start_date = from_date + timedelta(hours=i)
        to_date = start_date + timedelta(seconds=3600)
        res_file = create_block_csv_resu(mp, start_date, to_date)
        print(res_file.getvalue())
        mete_file = create_block_csv_mete(mp, start_date, to_date)
        print(mete_file.getvalue())
        terz_file = create_block_csv_terz(mp, start_date, to_date)
        # print(terz_file.getvalue())
    


