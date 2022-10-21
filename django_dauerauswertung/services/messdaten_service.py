import math
import pandas as pd
import datetime
import numpy as np
from constants import terzfrequenzen
from DTO import Messpunkt



def get_resudaten_single(mp_id: int, jahr: int, monat: int, tag: int, sekunde_from: int,
                            sekunde_to: int) -> pd.DataFrame:
    print(mp_id, sekunde_from, sekunde_to)
    dti = pd.date_range(
        datetime.datetime(jahr, monat, tag) + datetime.timedelta(seconds =sekunde_from),
        datetime.datetime(jahr, monat, tag) + datetime.timedelta(seconds=sekunde_to-1),
        freq="s", name="Timestamp")
    data_dict = {
        f"R{mp_id}_LAFeq": 15*np.random.rand(sekunde_to-sekunde_from) + 60,
        f"R{mp_id}_LAFmax": 15*np.random.rand(sekunde_to-sekunde_from) + 80,
        f"R{mp_id}_LCFeq": 15*np.random.rand(sekunde_to-sekunde_from) + 100
                    }
    df = pd.DataFrame(data_dict, index=dti)
    print(df)
    return df


def get_resu_all_mps(my_mps_data: list[Messpunkt], from_date, to_date):
    print(my_mps_data, from_date, to_date)
    ids_only = [mp.Id for mp in my_mps_data]
    return get_resudaten(ids_only, from_date.year, from_date.month, from_date.day, from_date.hour*3600, (to_date.hour)*3600+to_date.minute*60+to_date.second)


def get_terz_all_mps(my_mps_data: list[Messpunkt], from_date, to_date):
    ids_only = [mp.Id for mp in my_mps_data]
    return get_terzdaten(ids_only, from_date.year, from_date.month, from_date.day, from_date.hour*3600, (to_date.hour)*3600+to_date.minute*60+to_date.second)

def get_resudaten(ids_mps, jahr, monat, tag, sekunde_from, sekunde_to):
    df_mps = []
    for i in ids_mps:
        df_mps.append(get_resudaten_single(i, jahr, monat, tag, sekunde_from, sekunde_to))
    result = pd.concat(df_mps, axis=1, join="inner")
    return result

def get_terzdaten(ids_mps, jahr, monat, tag, sekunde_from, sekunde_to):
    df_mps = []
    for i in ids_mps:
        df_mps.append(get_terzdaten_single(i, jahr, monat, tag, sekunde_from, sekunde_to))
    result = pd.concat(df_mps, axis=1, join="inner")
    return result

def get_terzdaten_single(mp_id, jahr, monat, tag, sekunde_from, sekunde_to) -> pd.DataFrame:
    terz_prefix = "LZeq"
    available_cols_terz = []
    cols_terz = []
    values_terz = []
    for k in terzfrequenzen:
        available_cols_terz.append(terz_prefix + k)
    t_arr = [f"""T{mp_id}"""]  # "T2", "T3", "T4", "T5", "T6"]
    for i in available_cols_terz:
        for j in t_arr:
            cols_terz.append(f"{j}_{i}")
            values_terz.append(5 * np.random.rand(sekunde_to - sekunde_from) + 50)
    dti = pd.date_range(
        datetime.datetime(jahr, monat, tag) + datetime.timedelta(seconds=sekunde_from),
        datetime.datetime(jahr, monat, tag) + datetime.timedelta(seconds=sekunde_to - 1),
        freq="s",
        name="Timestamp")
    data_dict = dict(zip(cols_terz, values_terz))
    df = pd.DataFrame(data_dict, index=dti)
    if False:
        df.rename(columns={"hz20": f"T{mp_id}_LZeq20", "hz25": f"T{mp_id}_LZeq25", "hz31_5": f"T{mp_id}_LZeq31_5", "hz40": f"T{mp_id}_LZeq40", "hz50": f"T{mp_id}_LZeq50", "hz63": f"T{mp_id}_LZeq63", "hz80": f"T{mp_id}_LZeq80", "hz100": f"T{mp_id}_LZeq100", "hz125": f"T{mp_id}_LZeq125", "hz160": f"T{mp_id}_LZeq160",
                                "hz200": f"T{mp_id}_LZeq200", "hz250": f"T{mp_id}_LZeq250", "hz315": f"T{mp_id}_LZeq315", "hz400": f"T{mp_id}_LZeq400", "hz500": f"T{mp_id}_LZeq500", "hz630": f"T{mp_id}_LZeq630", "hz800": f"T{mp_id}_LZeq800", "hz1000": f"T{mp_id}_LZeq1000", "hz1250": f"T{mp_id}_LZeq1250", "hz1600": f"T{mp_id}_LZeq1600",
                                "hz2000": f"T{mp_id}_LZeq2000", "hz2500": f"T{mp_id}_LZeq2500", "hz3150": f"T{mp_id}_LZeq3150", "hz4000": f"T{mp_id}_LZeq4000", "hz5000": f"T{mp_id}_LZeq5000", "hz6300": f"T{mp_id}_LZeq6300", "hz8000": f"T{mp_id}_LZeq8000", "hz10000": f"T{mp_id}_LZeq10000", "hz12500": f"T{mp_id}_LZeq12500", "hz16000": f"T{mp_id}_LZeq16000",
                                "hz20000": f"T{mp_id}_LZeq20000"}, inplace=True)
    return df


def read_mete_data_v1(from_date, to_date):
    return get_metedaten(from_date.year, from_date.month, from_date.day, from_date.hour*3600, (to_date.hour)*3600+to_date.minute*60+to_date.second)

def get_metedaten(jahr, monat, tag, sekunde_from, sekunde_to) -> pd.DataFrame:
    cols_mete = ["Regen", "MaxWindgeschwindigkeit", "Windrichtung"]
    values_mete = [np.floor(np.random.rand(sekunde_to - sekunde_from) + 0.001), 2 * np.random.rand(sekunde_to - sekunde_from) + 1.65,
                    np.floor(360*np.random.rand(sekunde_to - sekunde_from))]
    dti = pd.date_range(
        datetime.datetime(jahr, monat, tag) + datetime.timedelta(seconds=sekunde_from),
        datetime.datetime(jahr, monat, tag) + datetime.timedelta(seconds=sekunde_to - 1),
        freq="s", name="Timestamp")
    data_dict = dict(zip(cols_mete, values_mete))
    df = pd.DataFrame(data_dict, index=dti)

    return df





if __name__ == "__main__":
    print(get_resudaten(2022, 12, 1, 2*3600, 3*3600))
    print(get_terzdaten(2022, 12, 1, 2*3600, 3*3600))
    print(get_metedaten(2022, 12, 1, 2*3600, 3*3600))