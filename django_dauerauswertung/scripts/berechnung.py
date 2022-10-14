import math
import sys


import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import logging

from requests import HTTPError
import requests
from models.auswertung import Vorbeifahrt, Aussortiert, LautesteStunde, Schallleistungspegel, Auswertungslauf
from models.allgemein import Koordinaten, Immissionsort, Messpunkt, Projekt
from influx_api import delete_old_zug_data
from models.auswertung import Detected
from push_messdaten_to_influx_database import prepare_erkennung

from utility import get_interval_beurteilungszeitraum_from_datetime, get_id_corresponding_beurteilungszeitraum

from utility import umrechnung_Z_2_A, terzfrequenzen


from konfiguration import project_mannheim, project_immendingen

# from messdaten_04_22 import *
from messdaten_07_22 import get_resu_all_mps, get_terz_all_mps, read_mete_data_v1

# from dtaidistance import dtw
# from dtaidistance.subsequence.dtw import subsequence_alignment, SubsequenceAlignment
# from dtaidistance import dtw_visualisation as dtwvis
from scipy import stats

import stumpy

def erstelle_ergebnis_df(originators, jahr, monat, tag, sekunde_from, sekunde_to, io, mps: list[Messpunkt]):
    # not_used = [str(mp.Id) + "_" + erg for mp in mps for erg in mp.Ereignisse]
    # originators = not_used # [mp.id for mp in mps]
    dti = pd.date_range(
        datetime(jahr, monat, tag) + timedelta(seconds=sekunde_from+299),
        datetime(jahr, monat, tag) + timedelta(seconds=sekunde_to),
        freq="300s", name="Timestamp")
    len_dti = len(dti)
    my_dict = dict(zip([i for i in originators], [np.zeros(len_dti) for i in originators]))
    # df = pd.DataFrame({"toDrop": np.zeros(len(dti))}, index=dti)
    df = pd.DataFrame(my_dict, index=dti)
    return df


def simple_filter_mp_column_in_measurement_data_12_21(mp_id, column_name_without_prefix, threshold, all_data):
    # no side effects
    logging.debug(f"all_data: {all_data}")
    aussortiere_zeilen = all_data[f"""R{mp_id}_{column_name_without_prefix}"""] >= threshold
    return aussortiere_zeilen


def berechne_schalldruckpegel_von_verursacher_an_io_12_21(io: Immissionsort, mp: Messpunkt, ereignis: str, ausbreitungsfaktor_originator_zu_io, df, has_mete = True):
    io_koordinaten = io.Koordinaten
    mp_koordinaten = mp.Koordinaten
    originator = str(mp.Id) + "_" + ereignis
    logging.debug("Start")
    if has_mete:
        windkorrektur_io_originator = berechne_windkorrektur_05_21(io_koordinaten, mp_koordinaten, df)
    else:
        windkorrektur_io_originator = 0
    schalldruckpegel_an_io_von_originator = df[f"mp{originator}_rechenwert_auswertung"]\
                                    + windkorrektur_io_originator + ausbreitungsfaktor_originator_zu_io
    logging.debug(schalldruckpegel_an_io_von_originator)
    return schalldruckpegel_an_io_von_originator


def berechne_hoechste_lautstaerke_an_io_12_21(io: Immissionsort, mp: Messpunkt, ausbreitungsfaktor_originator_zu_io: float, df, has_mete=False):
    io_koordinaten = io.Koordinaten
    mp_koordinaten = mp.Koordinaten
    collection_lautester_pegel_io_von_mp = {}
    if has_mete:
        windkorrektur_mp = berechne_windkorrektur_05_21(io_koordinaten, mp_koordinaten, df)
    else:
        windkorrektur_mp = 0
    # windkorrektur_mp = berechne_windkorrektur_05_21(io_koordinaten, mp_koordinaten, df)
    lafmax_mp_mit_windkorrektur = df[f"""R{mp.Id}_LAFmax"""] + windkorrektur_mp
    lafmax_mp_mit_windkorrektur_ausbreitungsfaktor = lafmax_mp_mit_windkorrektur + ausbreitungsfaktor_originator_zu_io
    return lafmax_mp_mit_windkorrektur_ausbreitungsfaktor


def berechne_schallleistungpegel_an_mp_12_21(mp, df, korrekturfaktor):
    result_df = (df[f"""R{mp}_LAFeq"""].groupby(by=lambda r: r.hour).max() + korrekturfaktor)
    logging.info(f'TEST: {result_df}')
    # result = 10*math.log10(np.power(10, 0.1*df[f"""R{mp}_LAFmax"""]).mean())
    # logging.debug(f"schallleistungpegel {result} resulting in {result + korrekturfaktor}")
    return result_df

### DEPRECATED
def berechne_beurteilungspegel_von_verursacher_an_io_12_21(io, originator, df, rechenwert_verwertbare_sekunden):
    modifizierter_schalldruck = berechne_schalldruckpegel_von_verursacher_an_io_12_21(io, originator, df)
    energie_aufsummiert = modifizierter_schalldruck.apply(lambda i: 10**(0.1*i)).cumsum()
    beurteilungspegel_von_mp = (energie_aufsummiert/rechenwert_verwertbare_sekunden).apply(lambda i: 10*math.log10(i))
    return beurteilungspegel_von_mp


def berechne_beurteilungspegel_an_io_12_21(df, rechenwert_verwertbare_sekunden, io: Immissionsort):
    assert rechenwert_verwertbare_sekunden != 0
    as_energy = df.apply(lambda i: 10 ** (0.1 * i))
    if io.ruhezeitzuschlag:
        as_energy["zuschlag"] = 0
        for h in [6, 20, 21]:
            try:
                min_el_of_hour = as_energy[as_energy.index.hour == h]["zuschlag"].idxmin()
                as_energy.loc[pd.to_datetime(min_el_of_hour), "zuschlag"] = as_energy.loc[pd.to_datetime(min_el_of_hour), "zuschlag"] + 10**(6*0.1)
            except ValueError:
                logging.debug(f"Hour {h} not available")

    energie_aufsummiert = as_energy.cumsum()
    logging.debug(f"energie_aufsummiert {energie_aufsummiert}")
    energie_aufsummiert["gesamt"] = energie_aufsummiert.sum(axis=1)
    if io.ruhezeitzuschlag:
        energie_aufsummiert = energie_aufsummiert.drop(columns=["zuschlag"], axis=1)
    beurteilungspegel_alle_verursacher_and_gesamt = 10 * np.log10((energie_aufsummiert / rechenwert_verwertbare_sekunden))
    
    logging.debug(f"beurteilungspegel_alle_verursacher_and_gesamt {beurteilungspegel_alle_verursacher_and_gesamt}")
    logging.debug(f"beurteilungspegel_alle_verursacher_and_gesamt.columns {beurteilungspegel_alle_verursacher_and_gesamt.columns}")
    return beurteilungspegel_alle_verursacher_and_gesamt



def berechne_verwertbare_sekunden(df):
    # latest_index = df.index[-1]
    # logging.debug(f"latest index: {latest_index}")
    hours_with_number_values = df.resample('h').size()
    logging.debug(f"hours_with_number_values: {hours_with_number_values}")
    hours_with_enough_usable_seconds = hours_with_number_values >= 900
    total_number_usable_seconds = hours_with_number_values[hours_with_number_values >= 900].apply(lambda i: 3600).sum()
    logging.debug(f"total_number_usable_seconds: {total_number_usable_seconds}")
    logging.debug(f"hours_with_enough_usable_seconds: {hours_with_enough_usable_seconds}")
    hours_with_less_usable_seconds = hours_with_enough_usable_seconds[-hours_with_enough_usable_seconds]
    my_arr = []
    logging.debug(hours_with_less_usable_seconds.index.hour)
    logging.debug(df.index.hour)
    samplesize_filtered_df = df
    samplesize_deleted = []
    for h in hours_with_less_usable_seconds.index.hour:
        if not samplesize_filtered_df[samplesize_filtered_df.index.hour == h].empty:
            samplesize_deleted.append(samplesize_filtered_df[samplesize_filtered_df.index.hour == h])
            logging.info(f"Dropping of hour {h} because there is too few data...")
        samplesize_filtered_df = samplesize_filtered_df[samplesize_filtered_df.index.hour != h]
        
    if len(samplesize_deleted) > 0:
        samplesize_deleted_df = pd.concat(samplesize_deleted)
        logging.debug(f"samplesize_filtered_df: {samplesize_filtered_df}")
        return total_number_usable_seconds.item(), samplesize_filtered_df, samplesize_deleted_df
    else:
        return total_number_usable_seconds.item(), samplesize_filtered_df, pd.DataFrame()


def korrigiere_windeinfluss(winkel_verbindungslinie_mp_io_windrichtung, windgeschwindigkeit):
    grenzwert = 0.5
    # eps:= Winkel zwischen Verbindungslinie IO_MP und Richtung, in die der Wind weht(180° + Windrichtung)
    eps = winkel_verbindungslinie_mp_io_windrichtung
    if windgeschwindigkeit < grenzwert:
        return 0
    else:
        # Bemerkung: Maximale Dämpfung (von -10 dB) bei eps = 180
        # Minimale Dämpfung (von 0 dB) bei eps = 0
        return -1 * (5 - 5 * math.cos((eps / 360 * 2 * math.pi - math.pi / 4 * math.sin(eps / 360 * 2 * math.pi))))


def berechne_windkorrektur_05_21(koordinaten_mp, koordinaten_io, df_mete):
    winkel_io_mp = berechne_winkel_io_mp_12_21(koordinaten_mp, koordinaten_io)
    # logging.debug(f"Berechneter Winkel: {winkel_io_mp}")
    result = df_mete.apply(lambda i: korrigiere_windeinfluss(
        winkel_io_mp + 180 + i["Windrichtung"], i["MaxWindgeschwindigkeit"]), axis=1)
    # logging.debug("Windkorrektur")
    # logging.debug(result.min())
    return result


def berechne_winkel_io_mp_12_21(koordinaten_mp: Koordinaten, koordinaten_io: Koordinaten):
    '''Berechnet den Winkel zwischen arg1 und arg2 aus Sicht der Nordrichtung (gegen den Urzeigersinn)'''
    # aus stackoverflow: direct-way-of-computing-clockwise-angle-between-2-vectors
    # Bemerkung: Skalarprodukt zeigt Richtung mit an, d.h. (Winkel zwischen v0, v1) = - (Winkel zwischen v1, v0)
    # logging.debug("Winkelberechnung")
    # v0 = [koordinaten_io["rechtswert"], koordinaten_io["hochwert"]]
    # v1 = [koordinaten_mp["rechtswert"], koordinaten_mp["hochwert"]]
    #
    verbindung_io_mp = [koordinaten_io.GKRechtswert - koordinaten_mp.GKRechtswert,
                        koordinaten_io.GKHochwert - koordinaten_mp.GKHochwert]
    vektor_nach_norden = [0, 1]
    angle = np.math.atan2(np.linalg.det([vektor_nach_norden, verbindung_io_mp]),
                          np.dot(vektor_nach_norden, verbindung_io_mp))
    #
    # return angle  # im Bogenmaß
    logging.debug(f"{angle} rad, {np.degrees(angle)} °")
    return np.degrees(angle)


def filter_vogel_12_21(mp_id, df):
    key_list = list(map(lambda j: f"""T{mp_id}_LZeq{j}""", terzfrequenzen))
    value_list = list(map(lambda j: f"""LZeq{j}""", terzfrequenzen))
    rename_dict = dict(zip(key_list, value_list))

    columns_of_messpunkt_in_terz = [col for col in df if col.startswith(f"""T{mp_id}""")]
    terz_daten = df[columns_of_messpunkt_in_terz]
    terz_daten_a_bewertet = terz_daten + umrechnung_Z_2_A
    terz_daten_energetic = terz_daten_a_bewertet.apply(lambda x: 10 ** (0.1 * x))
    renamed_terz_daten_energetic = terz_daten_energetic.rename(columns=rename_dict)
    vogel_frequenzen = ["LZeq2000", "LZeq2500", "LZeq3150", "LZeq4000", "LZeq5000", "LZeq6300", "LZeq8000"]
    a_f = renamed_terz_daten_energetic.sum(1).apply(lambda x: 10 * math.log10(x))
    v_f = renamed_terz_daten_energetic[vogel_frequenzen].sum(1).apply(lambda x: 10 * math.log10(x))
    # Sekunde werten, wenn Gesamtpegel aller Frequenzen >= Vogelfrequenzen + 1.5
    sekunden_ohne_vogeleinfluss = a_f >= v_f + 1.5
    # sekunden_mit_vogel = terz_daten_energetic[a_f < v_f + 1.5]
    return sekunden_ohne_vogeleinfluss


def find_vorbeifahrt_mp_5_immendingen(mp: Messpunkt, df) -> list[Vorbeifahrt]:
    mp_id = mp.Id
    vorbeifahrten_container = []
    df[
        f"""mp{mp_id}_ohne_ereignis_rechenwert_auswertung"""].rolling(20).apply(
        lambda j: erkenne_vorbeifahrt(j, vorbeifahrten_container, mp))
    logging.debug(f"vorbeifahrten_container {vorbeifahrten_container}")
    return vorbeifahrten_container



def find_and_modify_grillen(mp_id, df):
    key_list = list(map(lambda j: f"""T{mp_id}_LZeq{j}""", terzfrequenzen))
    value_list = list(map(lambda j: f"""LZeq{j}""", terzfrequenzen))
    rename_dict = dict(zip(key_list, value_list))
    columns_of_messpunkt_in_terz = [col for col in df if col.startswith(f"""T{mp_id}""")]
    terz_daten = df[columns_of_messpunkt_in_terz]
    terz_daten_a_bewertet = terz_daten + umrechnung_Z_2_A
    terz_daten_energetic = terz_daten_a_bewertet.apply(lambda x: 10 ** (0.1 * x))
    renamed_terz_daten_energetic = terz_daten_energetic.rename(columns=rename_dict)

    niedrige_frequenzen = ["LZeq20", "LZeq25", "LZeq31_5", "LZeq40", "LZeq50", "LZeq63", "LZeq80", "LZeq100",
                           "LZeq125", "LZeq160", "LZeq200", "LZeq250", "LZeq315", "LZeq400", "LZeq500", "LZeq630",
                           "LZeq800", "LZeq1000", "LZeq1250", "LZeq1600", "LZeq2000", "LZeq2500", "LZeq3150",
                           "LZeq4000", "LZeq5000"]
    grillen_frequenzen = ["LZeq6300", "LZeq8000", "LZeq10000", "LZeq12500"]
    n_f = renamed_terz_daten_energetic[niedrige_frequenzen].sum(1).apply(lambda x: 10 * math.log10(x))
    g_f = renamed_terz_daten_energetic[grillen_frequenzen].sum(1).apply(lambda x: 10 * math.log10(x))
    sekunden_ohne_grillen = n_f >= g_f
    return n_f[-sekunden_ohne_grillen]

def filter_zug(mp_id: int, df : pd.DataFrame):
    key_list = list(map(lambda j: f"""T{mp_id}_LZeq{j}""", terzfrequenzen))
    value_list = list(map(lambda j: f"""LZeq{j}""", terzfrequenzen))
    rename_dict = dict(zip(key_list, value_list))
    columns_of_messpunkt_in_terz = [col for col in df if col.startswith(f"""T{mp_id}""")]
    terz_daten = df[columns_of_messpunkt_in_terz]
    terz_daten_a_bewertet = terz_daten + umrechnung_Z_2_A
    terz_daten_energetic = 10 ** (0.1*terz_daten_a_bewertet)
    low_terz_data_in_a_bewertung = terz_daten_energetic.iloc[:,0:12]
    mid_terz_data_in_a_bewertung =  terz_daten_energetic.iloc[:,12:23]
    high_terz_data_in_a_bewertung = terz_daten_energetic.iloc[:,23:31]


    t0 = 20*np.log10(mid_terz_data_in_a_bewertung.sum(axis=1))
    t1 = -10*np.log10(low_terz_data_in_a_bewertung.sum(axis=1)+high_terz_data_in_a_bewertung.sum(axis=1))
    t2 = -10*np.log10(terz_daten_energetic.sum(axis=1))

    # print(t0+t1+t2)
    sekunden_mit_zug = t0+t1+t2 >= 10
    print("Zug bei: ", sekunden_mit_zug[sekunden_mit_zug])
    return sekunden_mit_zug


s1 = [-1.449098067887945, -1.2889086556214204, -1.449098067887945, -1.3850223029813344, -1.0646434784482852, -0.5520373591954082, 0.537250644216961, 1.0818946459231433, 0.9857809985632294, 0.9857809985632294, 0.8896673512033133, 0.9217052336566187, 1.2100461757363625, 1.3702355880028871, 1.338197705549584, 0.9217052336566187, 0.4411369968570448, 0.345023349497131, 0.345023349497131, 0.2809475845905202, 0.12075817232399562, -0.2636964171156621, -0.5520373591954082, -1.0005677135416766, -1.2889086556214204, -1.4811359503412505]

# print(s1)
# df_z_normalized_smoothed = df_z_normalized.rolling('5s').mean()

def detect_zug_mannheim(mp_id, df_resu, large_groups, save_pic = False, threshold = 2.8):
    detections = []
    iteration = 0
    id_counter = 0
    for idx, r in large_groups.iterrows():
        iteration += 1
        # print("Group", r["min"], idx)
        under_investigation = df_resu[r["min"]:r["max"]][f"R{mp_id}_LAFeq"]
        logging.info(f"index: {idx}, {r['min']}")
        # apply_criteria_on_dt(under_investigation)
        matched_with_subsequence = False
        if True:
            penalty = 0.5
            # psi = min(int(len(under_investigation) / 2), int(len(s1) / 2))
            s2 = stats.zscore(under_investigation.to_numpy())
            # window_diff = np.diff(s2)
            # moderate_change_area = window_diff[(window_diff >= -0.2) & (window_diff < 0.2)]
            path = dtw.warping_path(s1, s2, penalty=penalty)
            if save_pic:
                dtwvis.plot_warping(s1, s2, path, filename=f"wraps/warp_{r['min'].strftime('%Y_%m_%d_%H_%M_%S')}.png")
            # print(s2.tolist())
            distance = dtw.distance(s1, s2, penalty=penalty)
            length_adjusted_distance = distance - 0.03*len(s2)
            if length_adjusted_distance <= threshold:
                pass
                # print("Match by dtw", distance)
            else:
                pass
                # print("Discard by dtw", distance)

        if length_adjusted_distance <= threshold:
            if iteration % 2 == 0:
                color = 'lightcoral'
            else:
                color = 'red'
        else:
            if iteration % 2 == 0:
                color = 'blue'
            else:
                color = 'green'

        
        # df_resu[r["min"]:r["max"]]["lafeq"].plot( label=f'{r["min"]}:{r["max"]}', color=color)
        if length_adjusted_distance <= threshold:
            detections.append(Detected(r["min"], r["max"], df_resu[r["min"]:r["max"]].index, 0, 1, id_counter, length_adjusted_distance))
            id_counter += 1
        # df_resu = df_resu.loc[(df_resu.index < r["min"]) | (df_resu.index > r["max"])]
        
    # print(f"Zum Ende: {len(df_resu)}")
    return detections


def filter_zug_v2(mp_id: int, df):
    df_rolling_mean = df[f"R{mp_id}_LAFeq"].rolling('600s').mean()
    df_difference = df[(df[f"R{mp_id}_LAFeq"] - df_rolling_mean) >= 0.25]


    df_reduced = df_difference.reset_index()

    df_reduced['grp_date'] = df_reduced["Timestamp"].diff().dt.seconds.ne(1).cumsum()
    # print(df_reduced)
    after_grouping = df_reduced.groupby(['grp_date'])["Timestamp"].agg(['min', 'max', 'count']) # apply(lambda x: x.iloc[[0, -1]]))

    # print(after_grouping)

    large_grous = after_grouping[after_grouping["count"] >= 14]
    # print("Large groups:", large_grous)


    # df_difference["lafeq"].resample('s').asfreq().plot()
    # df["lafeq"].rolling('5s').mean().plot()
    # df_z_normalized_smoothed["lafeq"].plot()
    # another_diff = df_z_normalized_smoothed["lafeq"].diff()
    # another_diff[abs(another_diff) >= 0.35].plot(marker='*', lw=0)
    # apply_subsequence_on_dt(df_z_normalized_smoothed)
    return detect_zug_mannheim(mp_id, df, large_grous)

def filter_zug_v3(mp_id: int, df: pd.DataFrame):
    query_motif = [55.1, 55.1, 55.1, 55.1, 56.1, 56.1,56.8,
 57.6,60.3,62.9,63.6,65.4,68.8,70.3,71.2,71.4,71.1,70.1,69.5,69.6,69.5,
 69.4,69.4,70.1,70. ,69.7,69.4,68. ,67.6,68.4,68.7,69.1,69.1,69.4,69.1,
 69.3,69.2,68.9,67.8,65.5,63.6,60.7,59.6,58.8,58.3,57.8,57.9,57.3,56.5, 56.5, 56.5, 56.5]

    shorter_motif = [55.1, 55.1, 55.1, 55.1, 56.1, 56.1,56.8,
 62.9,63.6,65.4,68.8,69.5,69.6,69.5,
 69.4,69.4,70.1,70. ,69.7,69.4,68., 69.1,
 69.3,69.2,68.9,67.8,65.5,63.6,60.7,59.6,56.5, 56.5, 56.5, 56.5]

    
    long_motif = [53.1,53.4,53.4,53.3,53.7,53.7,53.6,53.6,54.1,54.7,54.7,55.7,56.1,56.7,
 57.8,61.1,61.2,62.5,63.5,65.1,66.2,67.3,68.6,68.8,68.3,69. ,68.2,67.4,
 66.7,66.9,65.5,64.9,64.3,64.3,64.1,64. ,63.5,63.4,63.3,63.5,63.2,63.2,
 63.4,63.6,63.2,62.9,62.4,62.7,62.2,62.1,62.5,63. ,63.1,63.5,63.2,62.7,
 63.1,62.8,63. ,62.5,62.3,61.6,61.1,61.2,59.2,58.7,57.6,56.9,56.2,55.4,
 54.7,54.4,54.2,54.1,53.7,53.4,53.2,53.2,52.9,53.2]

    very_short_motif = [57.2,58.4,59.4,60.3,61.5,63.1,63.4,63.7,63.7,63.7,63.5,64.1,64. ,64.8,
 63.4,61.2,59.6,58.7,58.4,58.3,58. ]

    another_short_motif = [47.7,47.9,47.8,47.9,47.9,48. ,48.4,48.3,48.3,48.5,48.9,49.4,49.9,50.8,
 50.9,51.5,51.8,51.4,51.2,51.9,52.3,56.1,60. ,60.7,60.1,58.4,56.4,54. ,
 52.4,50.4,49. ,48.6,48.4,48.3,48.5,48.5,48.2]

    another_very_short_motif = [47.9,51.1,59.4,57.7,59.1,57.6,63.2,66. ,63.3,51.7]

    another_long_motif = [57.7,57.6,58.6,64.6,64.8,69.2,76.4,72.4,67.4,68.6,69.2,68.9,69.9,68.5,
 68.5,67.4,67.6,66.7,66. ,65.4,64.9,65.3,64.9,64.7,65.3,65.7,66.3,67.6,
 68.8,68.5,65.8,66.8,66.8,67.1,67.3,67.4,68.3,68.1,67.5,67.1,65.9,64.3,
 62.7,61.5,60.5,59.9,59.3,58.8,57.3,56.9,57.2]

    far_off_rail_motif = [55.3,56. ,55.2,55. ,56. ,55.7,55.5,55.4,55.6,55.1,55.4,55.5,55.7,55.4,
 56.3,56.7,57.4,58.6,62.5,65.4,66.5,68.1,70.3,68.7,70.2,71.2,70. ,66.5,
 64. ,60.6,58.3,57.6,57. ,55.9,56.1,55.9,56.2,55.5,56. ,55.3,55.3,55.8,
 55.1,55.2,55.8,54.8,54.8,55.2,55.5,56.3,56.7]


    df_reset_index = df.reset_index()

    df_rolling_mean = df[f"R{mp_id}_LAFeq"].rolling('600s').mean()

    np_arr = df.loc[:, f"R{mp_id}_LAFeq"].to_numpy()

    np_arr[np_arr <= np_arr.mean() + 1.5] = np_arr.mean()
    motif_counter = 0
    current_pos = 0
    detections = []
    id_counter = 0
    for arg in [
        (query_motif, "silver", 4.5, 1), 
        (shorter_motif, "lightgreen", 3.7, 2),
        (long_motif, 'lightblue', 5, 3), 
        # (very_short_motif, "LightCoral", 3), (another_short_motif, "Khaki", 2.5),
        # (another_very_short_motif, "Fuchsia", 1.4),
        (another_long_motif, "Orange", 5.5, 4),
        (far_off_rail_motif, "teal", 3.5, 5)]:

        motif_matches = stumpy.match(arg[0], np_arr)
        tol = 8

        current_pos += len(arg[0])

        
        for r in motif_matches:
            motif_counter += 1
            print(r[1])
            
            start_detection = r[1]
            end_detections = r[1]+len(arg[0])
            print(start_detection, end_detections)
            print(df_reset_index.iloc[start_detection]["Timestamp"])
            if r[0] <= arg[2]:

                detections.append(Detected(df_reset_index.iloc[start_detection]["Timestamp"], df_reset_index.iloc[end_detections]["Timestamp"], df_reset_index.iloc[start_detection:end_detections]["Timestamp"], arg[3], mp_id, id_counter, r[0]))
                id_counter += 1
    return detections



def filter_wind_12_21(mete_data):
    bezeichnung_rolling_column = "RollingWind"
    rolling = mete_data["MaxWindgeschwindigkeit"].rolling('30s').max().shift(-15, 's')
    rolling.name = bezeichnung_rolling_column
    joined = mete_data.join(rolling).fillna(method='ffill')
    sekunden_mit_windeinfluss = joined[bezeichnung_rolling_column] > 3.6
    return sekunden_mit_windeinfluss


def filter_regen_12_21(mete_data):
    rolling = mete_data["Regen"].rolling('60s').sum().shift(-30, 's')
    rolling.name = "RollingRegen"
    joined = mete_data.join(rolling).fillna(method='ffill')
    sekunden_mit_regeneinfluss = joined["RollingRegen"] > 0
    return sekunden_mit_regeneinfluss


def erkenne_vorbeifahrt(param, result_container, mp: Messpunkt):
    delta_time = timedelta(seconds=5)
    if not (param is None) and param[0] >= 60:
        my_argmax = param.argmax()
        my_max = param[my_argmax]
        first_part = param[0: my_argmax]
        second_part = param[my_argmax:]
        a = first_part < my_max - 1
        b = second_part < my_max - 1
        result_a = []
        for i, el_a in enumerate(a):
            latest_in_b_for_el_a = {"index_beginn_detection": param.index[i], "value": first_part[i],
                                    "index_ende_detection": None, "difference": None, "wertungsbeginn": None,
                                    "wertungsende": None}
            if el_a:
                for j, el_b in enumerate(b):
                    if el_b:
                        if ((param.index[j + my_argmax] - param.index[i]).seconds >= 8 and (
                                param.index[j + my_argmax] - param.index[i]).seconds < 20):
                            latest_in_b_for_el_a["index_ende_detection"] = param.index[j + my_argmax]
                            latest_in_b_for_el_a["difference"] = j + my_argmax - i
            result_a.append(latest_in_b_for_el_a)
        results_bereinigt = list(filter(lambda x: x["difference"] is not None, result_a))
        # return results_bereinigt
        if len(results_bereinigt) > 0:
            max_ergebnis = max(results_bereinigt, key=lambda x: x['difference'])  # Finde das Maximum
            max_ergebnis["wertungsbeginn"] = max_ergebnis["index_beginn_detection"] - delta_time
            max_ergebnis["wertungsende"] = max_ergebnis["index_ende_detection"] + delta_time
            # logging.debug("Vorbeifahrt erkannt")
            result_container.append(Vorbeifahrt(max_ergebnis["index_beginn_detection"] - delta_time,
                                                max_ergebnis["index_ende_detection"] + delta_time, mp))
            return 1
            # return param[max_ergebnis['index_left'] : my_argmax + max_ergebnis["index_right"]]
    # logging.debug("Kein Vorbeifahrt erkannt")
    return 0


def create_indicator_vorbeifahrt(df, list_vorbeifahrten):
    # my_date_range = pd.date_range(start=start_time, end=end_time, freq='S')
    indicator = pd.Series(data=np.full(len(df.index), False), index=df.index)
    for i, el in enumerate(list_vorbeifahrten):
        indicator[el.beginn:el.ende] = True
    return indicator


def create_complete_df(resu: pd.DataFrame, terz, mete, has_mete=False, has_terz=True):
    if has_terz:
        df_all_resu_all_terz = pd.merge(resu, terz, left_index=True, right_index=True)
        if has_mete:
            df_all_resu_all_terz_all_mete = pd.merge(df_all_resu_all_terz, mete, left_index=True, right_index=True)
            return df_all_resu_all_terz_all_mete
        else:
            return df_all_resu_all_terz
    else:
        df_all_resu = resu.copy()
        if has_mete:
            df_all_resu_all_mete = pd.merge(df_all_resu, mete, left_index=True, right_index=True)
            return df_all_resu_all_mete
        else:
            return df_all_resu


def create_df_with_rechenwerte(df, my_mps_data: list[Messpunkt]):
    logging.debug(df)
    ids_messpunkte = [mp.Id for mp in my_mps_data]
    key_list = list(map(lambda j: f"""R{j}_LAFeq""", ids_messpunkte))
    value_list = list(map(lambda j: f"""mp{j}_ohne_ereignis_rechenwert_auswertung""", ids_messpunkte))
    rename_dict = dict(zip(key_list, value_list))
    df_with_rechenwerte = df
    for j in ids_messpunkte:
        df_with_rechenwerte[f"mp{j}_ohne_ereignis_rechenwert_auswertung"] = df[f"R{j}_LAFeq"]
    return df_with_rechenwerte





def evaluate(project: Projekt, zeitpunkt_im_zielzeitraum: datetime, has_mete = False, lafeq_gw=90, lafmax_gw = 100, use_terz_data = True):
    # my_ios = [1, 5, 9, 15, 17]
    year = zeitpunkt_im_zielzeitraum.year
    month = zeitpunkt_im_zielzeitraum.month
    day = zeitpunkt_im_zielzeitraum.day
    seconds_start, seconds_end = get_interval_beurteilungszeitraum_from_datetime(zeitpunkt_im_zielzeitraum)
    zugeordneter_beurteilungszeitraum = get_id_corresponding_beurteilungszeitraum(zeitpunkt_im_zielzeitraum)
    logging.info(f"Beurteilungszeitraum: {zugeordneter_beurteilungszeitraum}")
    logging.info(f"has_mete: {has_mete}")

    my_ios: list[Immissionsort] = project.IOs
    my_mps_data: list[Messpunkt] = project.MPs #[mp for mp in mps_mit_ereignissen if mp.id in [1, 2, 3, 4, 5, 6]]
    abf_data= project.Ausbreitungsfaktoren

    alle_verursacher = ["mp" + str(mp.Id) + "_" + erg for mp in my_mps_data for erg in mp.Ereignisse]
    alle_verursacher.append("gesamt")

    from_date = datetime(year, month, day) + timedelta(seconds=seconds_start)
    to_date = datetime(year, month, day) + timedelta(seconds=seconds_end)

    my_auswertungslauf = Auswertungslauf(from_date, project.name, zugeordneter_beurteilungszeitraum)


    

    my_ergebnis = erstelle_ergebnis_df(alle_verursacher, year, month, day, seconds_start, seconds_end, 1, my_mps_data)
    try:
        logging.info("Loading data...")
        if use_terz_data:
            terz = get_terz_all_mps(project.name_in_db, my_mps_data, from_date, to_date)
        resu = get_resu_all_mps(project.name_in_db, my_mps_data, from_date, to_date)

        
        if has_mete:
            mete = read_mete_data_v1(project.name_in_db, from_date, to_date)
            if use_terz_data:
                data_as_one = create_complete_df(resu, terz, mete, has_mete)
            else:
                data_as_one = create_complete_df(resu, [], mete, has_mete, False)
        else:
            if use_terz_data:
                data_as_one = create_complete_df(resu, terz, [], has_mete)
            else:
                data_as_one = create_complete_df(resu, [], [], has_mete, False)

        logging.info("Finished loading ")
        df_with_rechenwert = create_df_with_rechenwerte(data_as_one, my_mps_data)

        # Filtern der Daten
        my_auswertungslauf.no_verfuegbare_messwerte = len(df_with_rechenwert)
        logging.info(f"Vor Filtern: {my_auswertungslauf.no_verfuegbare_messwerte}")
        aussortierung_set = []
        if has_mete:
            ausortiert_by_windfilter = filter_wind_12_21(df_with_rechenwert)
            df_with_rechenwert = df_with_rechenwert[-ausortiert_by_windfilter]
            # my_results_filter["wind"] = [] #ausortiert_by_windfilter[ausortiert_by_windfilter]
            aussortierung_set.append(Aussortiert(ausortiert_by_windfilter[ausortiert_by_windfilter],"wind", None))
            logging.debug(f"Nach Windfilter: {len(df_with_rechenwert)}")
            ausortiert_by_regen = filter_regen_12_21(df_with_rechenwert)
            df_with_rechenwert = df_with_rechenwert[-ausortiert_by_regen]
            # my_results_filter["regen"] = [] #ausortiert_by_regen[ausortiert_by_regen]
            aussortierung_set.append(Aussortiert(ausortiert_by_regen[ausortiert_by_regen],"regen", None))
            logging.debug(f"Nach Regenfilter: {len(df_with_rechenwert)}")
        for mp in my_mps_data:
            aussortiert_by_simple_filter = simple_filter_mp_column_in_measurement_data_12_21(mp.Id, "LAFeq", lafeq_gw, df_with_rechenwert)
            df_with_rechenwert = df_with_rechenwert[-aussortiert_by_simple_filter]
            aussortierung_set.append(Aussortiert(aussortiert_by_simple_filter[aussortiert_by_simple_filter], "lafeq", mp))

            aussortiert_by_simple_filter = simple_filter_mp_column_in_measurement_data_12_21(mp.Id, "LAFmax", lafmax_gw, df_with_rechenwert)
            df_with_rechenwert = df_with_rechenwert[-aussortiert_by_simple_filter]
            aussortierung_set.append(Aussortiert(aussortiert_by_simple_filter[aussortiert_by_simple_filter], "lafmax", mp))
            if "Zug" in mp.Filter:
                logging.info(f"Vor Zugfilter: {len(df_with_rechenwert)}")
                if False:
                    aussortiert_by_zugfilter = filter_zug(mp.Id, df_with_rechenwert)
                    df_with_rechenwert = df_with_rechenwert[-aussortiert_by_zugfilter]
                    print(f"Nach Zugfilter: {len(df_with_rechenwert)}")
                    aussortierung_set.append(Aussortiert(aussortiert_by_zugfilter[aussortiert_by_zugfilter], "zug", mp))
                if False:

                    detections = filter_zug_v2(mp.Id, df_with_rechenwert)
                    for d in detections:
                        d: Detected
                        aussortierung_set.append(Aussortiert(df_with_rechenwert.loc[(df_with_rechenwert.index >= d.start) & (df_with_rechenwert.index <= d.end)].index.to_series(), "Zug_V2", mp))
                        df_with_rechenwert = df_with_rechenwert.loc[(df_with_rechenwert.index < d.start) | (df_with_rechenwert.index > d.end)]
                    
                if True:
                    detections = filter_zug_v3(mp.Id, df_with_rechenwert)
                    for d in detections:
                        d: Detected
                        aussortierung_set.append(Aussortiert(df_with_rechenwert.loc[(df_with_rechenwert.index >= d.start) & (df_with_rechenwert.index <= d.end)].index.to_series(), "Zug_V2", mp))
                        df_with_rechenwert = df_with_rechenwert.loc[(df_with_rechenwert.index < d.start) | (df_with_rechenwert.index > d.end)]
                logging.info(f"Nach Zugfilter: {len(df_with_rechenwert)}")
            if use_terz_data:
                aussortiert_by_vogelfilter = filter_vogel_12_21(mp.Id, df_with_rechenwert)
                logging.debug(f"aussortiert_by_vogelfilter {aussortiert_by_vogelfilter}")
                df_with_rechenwert = df_with_rechenwert[aussortiert_by_vogelfilter]
                # my_results_filter[f"vogelMp{mp.id}"] = [] # aussortiert_by_vogelfilter[-aussortiert_by_vogelfilter]
                aussortierung_set.append(Aussortiert(aussortiert_by_vogelfilter[-aussortiert_by_vogelfilter], "vogel", mp))
                modifizierte_pegel_wegen_grillen = find_and_modify_grillen(mp.Id, df_with_rechenwert)

                df_with_rechenwert.loc[
                    modifizierte_pegel_wegen_grillen.index, f"""mp{mp.Id}_ohne_ereignis_rechenwert_auswertung"""]\
                    = modifizierte_pegel_wegen_grillen
                logging.debug(modifizierte_pegel_wegen_grillen)
                aussortierung_set.append(Aussortiert(modifizierte_pegel_wegen_grillen, "grille", mp))

        
        anzahl_verwertbare_sekunden, sample_size_filtered_df, aussortiert_wegen_sample_size = berechne_verwertbare_sekunden(df_with_rechenwert)
        logging.debug(f"aussortiert_wegen_sample_size: {aussortiert_wegen_sample_size}")
        aussortierung_set.append(Aussortiert(aussortiert_wegen_sample_size.index.to_series(), "zu wenige messwerte", None))
        df_with_rechenwert = sample_size_filtered_df
        my_auswertungslauf.no_gewertete_messwerte = anzahl_verwertbare_sekunden
        my_auswertungslauf.no_verwertbare_messwerte = len(df_with_rechenwert)

        beurteilungspegel_set = {}
        lautestestunde_set = []


        if anzahl_verwertbare_sekunden == 0:
            logging.warning("Keine Verwertbaren Sekunden")
            for io in my_ios:
                for el in alle_verursacher:
                    beurteilungspegel_set[f"{io.Id}"] = my_ergebnis[el].to_frame()
            my_auswertungslauf.beurteilungspegel_set = beurteilungspegel_set
            my_auswertungslauf.erkennung_set = []
            my_auswertungslauf.aussortierung_set = aussortierung_set
            my_auswertungslauf.lautestestunde_set = []
            my_auswertungslauf.schallleistungspegel_set = []
            return my_auswertungslauf
        erkennung_set = []
        for mp in my_mps_data:
            if "vorbeifahrt" in mp.Ereignisse:
                vorbeifahrten_an_mp = find_vorbeifahrt_mp_5_immendingen(mp, df_with_rechenwert)
                for i in vorbeifahrten_an_mp:
                    erkennung_set.append(i)

                
                vorbeifahrten_an_mp_indicator_series = \
                    create_indicator_vorbeifahrt(
                        df_with_rechenwert, vorbeifahrten_an_mp
                    )
                df_with_rechenwert[f"mp{mp.Id}_vorbeifahrt_rechenwert_auswertung"] = df_with_rechenwert[f"mp{mp.Id}_ohne_ereignis_rechenwert_auswertung"]
                df_with_rechenwert.loc[
                    vorbeifahrten_an_mp_indicator_series, f"mp{mp.Id}_ohne_ereignis_rechenwert_auswertung"] = 0
                df_with_rechenwert.loc[-vorbeifahrten_an_mp_indicator_series, f"mp{mp.Id}_vorbeifahrt_rechenwert_auswertung"] = 0
        schallleistungspegel_set = []
        for mp in my_mps_data:
            schallleistungspegel = berechne_schallleistungpegel_an_mp_12_21(
                mp.Id, df_with_rechenwert, mp.LWA
            )     
            for idx, val in schallleistungspegel.iteritems():
                corresponding_date = datetime(year, month, day) + timedelta(hours=idx)
                schallleistungspegel_set.append(Schallleistungspegel(val, corresponding_date , mp))
        
        for io in my_ios:
            # anzahl_verwertbare_sekunden = berechne_verwertbare_sekunden(df_with_rechenwert)
            schalldruckpegel_von_verursacher = {}
            lautstaerke_von_verursacher = {}
            
            for mp in my_mps_data:
                for ereignis in mp.Ereignisse:
                    originator = "mp" + str(mp.Id) + "_" + ereignis
                    # schalldruckpegel_von_verursacher[f"df_schalldruckpegel_von_{originator}"] = \
                    schalldruckpegel_von_verursacher[f"{originator}"] = \
                        berechne_schalldruckpegel_von_verursacher_an_io_12_21(io, mp, ereignis, abf_data[(io.Id, mp.Id)], df_with_rechenwert, has_mete=has_mete)
                    logging.debug("Schalldruckpegel from each verursacher")
                lautstaerke_von_verursacher[f"{mp.Id}"] = \
                    berechne_hoechste_lautstaerke_an_io_12_21(io, mp, abf_data[(io.Id, mp.Id)], df_with_rechenwert, has_mete=has_mete)

            df_lauteste_stunde_io_von_mp = pd.DataFrame(lautstaerke_von_verursacher)
            arg_max_index_lautstaerke_io = df_lauteste_stunde_io_von_mp.max(axis=1).idxmax()
            arg_max_column_lautstaerke_io = \
                df_lauteste_stunde_io_von_mp.loc[arg_max_index_lautstaerke_io, :].idxmax()
            # my_results_lauteste_stunde[f"io{io.id}"] = (arg_max_column_lautstaerke_io, arg_max_column_lautstaerke_io, df_lauteste_stunde_io_von_mp.loc[arg_max_index_lautstaerke_io, arg_max_column_lautstaerke_io])
            logging.info(f"Lautester Zeitpunkt: {arg_max_index_lautstaerke_io} mit Pegel: {df_lauteste_stunde_io_von_mp.loc[arg_max_index_lautstaerke_io, arg_max_column_lautstaerke_io]}")

            lautestestunde_set.append(LautesteStunde(df_lauteste_stunde_io_von_mp.loc[arg_max_index_lautstaerke_io, arg_max_column_lautstaerke_io], arg_max_index_lautstaerke_io, io))

            df_schalldruckpegel_from_each_verursacher = pd.DataFrame(schalldruckpegel_von_verursacher)
            logging.debug(f"df_schalldruckpegel_from_each_verursacher: {df_schalldruckpegel_from_each_verursacher}")
            df_lautstaerke_von_mp = pd.DataFrame(lautstaerke_von_verursacher)
            beurteilungspegel_an_io = berechne_beurteilungspegel_an_io_12_21(
                df_schalldruckpegel_from_each_verursacher, anzahl_verwertbare_sekunden, io)
            end_result_with_additional_column = my_ergebnis.join(beurteilungspegel_an_io, how="left", lsuffix="_to_be_dropped").fillna(method='ffill')
            end_result = end_result_with_additional_column.drop([f"{el}_to_be_dropped" for el in alle_verursacher], axis=1).fillna(-100)
            logging.debug(f"my_ergebnis an io {io}: {my_ergebnis}")
            logging.debug(f"ergebnis io {io.Id}: {end_result}")
            for index, row in end_result.iterrows():
                beurteilungspegel_set[f"{io.Id}"] = end_result
            logging.info(f"Lr an {io.Id}: {end_result.iloc[-1,:]}")
        my_auswertungslauf.beurteilungspegel_set = beurteilungspegel_set
        my_auswertungslauf.erkennung_set = erkennung_set
        my_auswertungslauf.aussortierung_set = aussortierung_set
        my_auswertungslauf.lautestestunde_set = lautestestunde_set
        my_auswertungslauf.schallleistungspegel_set = schallleistungspegel_set
        logging.info("Evaluation finsihed")
        return my_auswertungslauf
    except Exception as ex:
        logging.exception(ex)
        raise ex


if __name__ == '__main__':
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
    level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("eval.log"),
    logging.StreamHandler(sys.stdout)]
    )
    try:
        zeitpunkt_im_zielzeitraum = datetime(2022, 6, 7, 14, 30, 0)
        auswertungslauf = evaluate(project_immendingen, zeitpunkt_im_zielzeitraum, True)

        try:
            pass
                                            
        except HTTPError as ex:
            logging.exception(ex.response.content)
            logging.exception(ex.response)
    except Exception as ex:
        logging.exception(ex)