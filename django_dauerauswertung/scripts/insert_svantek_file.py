import logging
import re
from io import StringIO
import pandas as pd
import datetime as dt
import json

col_headers_resu_in_svantek_file = ["Time", 'LAFmax (Ch1, P1) [dB]', 'LAFeq (Ch1, P1) [dB]',
             'LCFeq (Ch1, P2) [dB]']

rename_columns_resu_from_file_for_api = {"Time": "Date/Time",
                         
                         'LAFeq (Ch1, P1) [dB]': "lafeq",
                         'LAFmax (Ch1, P1) [dB]': "lafmax",
                         'LCFeq (Ch1, P2) [dB]': "lcfeq"}

cols_terz = ["Time",
             'Leq (Ch1, 20.00 Hz) [dB]', 'Leq (Ch1, 25.00 Hz) [dB]',
             'Leq (Ch1, 31.50 Hz) [dB]', 'Leq (Ch1, 40.00 Hz) [dB]',
             'Leq (Ch1, 50.00 Hz) [dB]', 'Leq (Ch1, 63.00 Hz) [dB]',
             'Leq (Ch1, 80.00 Hz) [dB]', 'Leq (Ch1, 100.00 Hz) [dB]',
             'Leq (Ch1, 125.00 Hz) [dB]', 'Leq (Ch1, 160.00 Hz) [dB]',
             'Leq (Ch1, 200.00 Hz) [dB]', 'Leq (Ch1, 250.00 Hz) [dB]',
             'Leq (Ch1, 315.00 Hz) [dB]', 'Leq (Ch1, 400.00 Hz) [dB]',
             'Leq (Ch1, 500.00 Hz) [dB]', 'Leq (Ch1, 630.00 Hz) [dB]',
             'Leq (Ch1, 800.00 Hz) [dB]', 'Leq (Ch1, 1000.00 Hz) [dB]',
             'Leq (Ch1, 1.25 kHz) [dB]', 'Leq (Ch1, 1.60 kHz) [dB]',
             'Leq (Ch1, 2.00 kHz) [dB]', 'Leq (Ch1, 2.50 kHz) [dB]',
             'Leq (Ch1, 3.15 kHz) [dB]', 'Leq (Ch1, 4.00 kHz) [dB]',
             'Leq (Ch1, 5.00 kHz) [dB]', 'Leq (Ch1, 6.30 kHz) [dB]',
             'Leq (Ch1, 8.00 kHz) [dB]', 'Leq (Ch1, 10.00 kHz) [dB]',
             'Leq (Ch1, 12.50 kHz) [dB]', 'Leq (Ch1, 16.00 kHz) [dB]',
             'Leq (Ch1, 20.00 kHz) [dB]']

cols_terz_rename_dict = {
    'Time': 'Date/Time',
    'Leq (Ch1, 20.00 Hz) [dB]': 'LZeq 20 Hz',
    'Leq (Ch1, 25.00 Hz) [dB]': 'LZeq 25 Hz',
    'Leq (Ch1, 31.50 Hz) [dB]': 'LZeq 31,5 Hz',
    'Leq (Ch1, 40.00 Hz) [dB]': 'LZeq 40 Hz',
    'Leq (Ch1, 50.00 Hz) [dB]': 'LZeq 50 Hz',
    'Leq (Ch1, 63.00 Hz) [dB]': 'LZeq 63 Hz',
    'Leq (Ch1, 80.00 Hz) [dB]': 'LZeq 80 Hz',
    'Leq (Ch1, 100.00 Hz) [dB]': 'LZeq 100 Hz',
    'Leq (Ch1, 125.00 Hz) [dB]': 'LZeq 125 Hz',
    'Leq (Ch1, 160.00 Hz) [dB]': 'LZeq 160 Hz',
    'Leq (Ch1, 200.00 Hz) [dB]': 'LZeq 200 Hz',
    'Leq (Ch1, 250.00 Hz) [dB]': 'LZeq 250 Hz',
    'Leq (Ch1, 315.00 Hz) [dB]': 'LZeq 315 Hz',
    'Leq (Ch1, 400.00 Hz) [dB]': 'LZeq 400 Hz',
    'Leq (Ch1, 500.00 Hz) [dB]': 'LZeq 500 Hz',
    'Leq (Ch1, 630.00 Hz) [dB]': 'LZeq 630 Hz',
    'Leq (Ch1, 800.00 Hz) [dB]': 'LZeq 800 Hz',
    'Leq (Ch1, 1000.00 Hz) [dB]': 'LZeq 1000 Hz',
    'Leq (Ch1, 1.25 kHz) [dB]': 'LZeq 1250 Hz',
    'Leq (Ch1, 1.60 kHz) [dB]': 'LZeq 1600 Hz',
    'Leq (Ch1, 2.00 kHz) [dB]': 'LZeq 2000 Hz',
    'Leq (Ch1, 2.50 kHz) [dB]': 'LZeq 2500 Hz',
    'Leq (Ch1, 3.15 kHz) [dB]': 'LZeq 3150 Hz',
    'Leq (Ch1, 4.00 kHz) [dB]': 'LZeq 4000 Hz',
    'Leq (Ch1, 5.00 kHz) [dB]': 'LZeq 5000 Hz',
    'Leq (Ch1, 6.30 kHz) [dB]': 'LZeq 6300 Hz',
    'Leq (Ch1, 8.00 kHz) [dB]': 'LZeq 8000 Hz',
    'Leq (Ch1, 10.00 kHz) [dB]': 'LZeq 10000 Hz',
    'Leq (Ch1, 12.50 kHz) [dB]': 'LZeq 12500 Hz',
    'Leq (Ch1, 16.00 kHz) [dB]': 'LZeq 16000 Hz',
    'Leq (Ch1, 20.00 kHz) [dB]': 'LZeq 20000 Hz'
}

cols_mete = ["Time", 'Temperature [°C]',
             'Pressure [hPa]', 'Humidity [%]', 'Avg. wind speed [m/s]',
             'Wind direction [°]', 'Max wind speed [m/s]', 'Rain']
cols_mete_rename_dict = {"Time": "Date/Time",
                         'Temperature [°C]': "Temperatur",
                         'Pressure [hPa]': "Druck",
                         'Humidity [%]': "Luftfeuchtigkeit",
                         'Rain': "Regen",
                         # "Mitl. Windgeschwindigkeit",
                         'Avg. wind speed [m/s]': "AvgWindgeschwindigkeit",
                         'Wind direction [°]': "Windrichtung",
                         # "Max Wind-Geschwindigkeit"}
                         'Max wind speed [m/s]': "MaxWindgeschwindigkeit"
                         }

rename_cols_terz_4_json = {
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


def process_svantek_rtm(filepath: str):
    logging.info(filepath)
    logging.info("In from_file_with_mete_2_df")
    data_file = open(filepath)

    content = data_file.read()
    p = re.compile('\n')

    p_begin_block = re.compile('time his.*\nStart time.*\n', re.IGNORECASE)

    p_latest_line = re.compile("latest.*\n", re.IGNORECASE)
    p_end_block = re.compile("MEASUREMENT INFORMATION", re.IGNORECASE)

    m_begin = p_begin_block.search(content)
    m_end = p_end_block.search(content)

    m_latest_line = p_latest_line.search(content)

    data_block = content[m_begin.end():m_latest_line.start()] + \
        content[m_latest_line.end():m_end.start()]

    data_block_without_trailing_colon = re.sub(';\n', '\n', data_block)

    content_messdaten = StringIO(data_block_without_trailing_colon)
    df = pd.read_csv(content_messdaten, sep=";", parse_dates=[0])

    df_resu = df[col_headers_resu_in_svantek_file].rename(columns=rename_columns_resu_from_file_for_api)
    # df_resu["LCFmax"] = None
    # df_resu["LZFeq"] = None
    # df_resu["LZFmax"] = None
    df_resu.set_index("Date/Time", inplace=True)
    df_resu.dropna(inplace=True)
    df_resu.sort_index(inplace=True)

    df_mete = df[cols_mete].rename(columns=cols_mete_rename_dict)
    df_mete.set_index("Date/Time", inplace=True)
    df_mete.dropna(inplace=True)
    df_mete["Windrichtung"] = df_mete["Windrichtung"].astype(int)
    df_mete.rename(columns={
            "Temperatur": "temperature", "Druck": "pressure", "Luftfeuchtigkeit": "humidity",
            "Windrichtung": "winddirection", "MaxWindgeschwindigkeit": "windspeed", "Regen": "rain"}, 
            inplace=True)
    # logging.info(df_mete)
    df_mete.sort_index(inplace=True)


    df_terz = df[cols_terz].rename(columns=cols_terz_rename_dict)
    df_terz = df_terz.rename(columns=rename_cols_terz_4_json)
    df_terz.set_index("Date/Time", inplace=True)
    df_terz.dropna(inplace=True)
    df_terz.sort_index(inplace=True)

    return (df_resu, df_terz, df_mete)


def process_svantek_rt(filepath: str):
    logging.info(filepath)
    data_file_without_mete = open(filepath)
    content = data_file_without_mete.read()
    p = re.compile('\n')
    p_begin_block = re.compile('time his.*\nStart time.*\n', re.IGNORECASE)

    p_latest_line = re.compile("latest.*\n", re.IGNORECASE)
    p_end_block = re.compile("MEASUREMENT INFORMATION", re.IGNORECASE)

    m_begin = p_begin_block.search(content)
    m_end = p_end_block.search(content)

    data_block = content[m_begin.end():m_end.start()]

    data_block_without_trailing_colon = re.sub(';\n', '\n', data_block)

    content_messdaten = StringIO(data_block_without_trailing_colon)
    df = pd.read_csv(content_messdaten, sep = ";", parse_dates=[0])
    df_resu = df[col_headers_resu_in_svantek_file].rename(columns=rename_columns_resu_from_file_for_api)
    # df_resu["LCFmax"] = None
    # df_resu["LZFeq"] = None
    # df_resu["LZFmax"] = None
    df_resu.set_index("Date/Time", inplace=True)
    df_resu.dropna(inplace=True)
    df_resu.sort_index(inplace=True)
    
    df_terz = df[cols_terz].rename(columns=cols_terz_rename_dict)
    df_terz = df_terz.rename(columns=rename_cols_terz_4_json)
    df_terz.set_index("Date/Time", inplace=True)
    df_terz.dropna(inplace=True)
    df_terz.sort_index(inplace=True)

    return (df_resu, df_terz)







