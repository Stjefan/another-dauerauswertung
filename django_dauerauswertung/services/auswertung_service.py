import logging
from datetime import datetime, timedelta
from re import T
import sys
import pandas as pd
import numpy as np
import requests

from DTO import (Projekt, Immissionsort, 
DTO_LrPegel, DTO_Rejected, DTO_MaxPegel, DTO_Detected, DTO_SchallleistungPegel,
    Messpunkt, Auswertungslauf, Koordinaten, Detected, Vorbeifahrt, Aussortiert, Schallleistungspegel, LautesteStunde, Ergebnisse)
from constants import get_interval_beurteilungszeitraum_from_datetime, get_id_corresponding_beurteilungszeitraum, terzfrequenzen, umrechnung_Z_2_A, get_start_end_beurteilungszeitraum_from_datetime


from messdaten_service import get_resudaten, get_terzdaten, get_metedaten, get_resu_all_mps, get_terz_all_mps, read_mete_data_v1

import stumpy
import math
import scipy

from konfiguration import project_mannheim

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
    schalldruckpegel_an_io_von_originator = df[f"{originator}_rechenwert"]\
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
    lafmax_mp_mit_windkorrektur_ausbreitungsfaktor.name = f"MP{mp.Id}"
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
                logging.warning(f"Hour {h} not available")

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
    logging.debug(hours_with_less_usable_seconds.index.hour)
    samplesize_filtered_df = df
    samplesize_deleted = []
    for h in hours_with_less_usable_seconds.index.hour:
        if not samplesize_filtered_df[samplesize_filtered_df.index.hour == h].empty:
            samplesize_deleted.append(samplesize_filtered_df[samplesize_filtered_df.index.hour == h])
            logging.debug(f"Dropping of hour {h} because there is too few data...")
        samplesize_filtered_df = samplesize_filtered_df[samplesize_filtered_df.index.hour != h]
        
    if len(samplesize_deleted) > 0:
        samplesize_deleted_df = pd.concat(samplesize_deleted)
        return total_number_usable_seconds.item(), samplesize_filtered_df, samplesize_deleted_df
    else:
        return total_number_usable_seconds.item(), samplesize_filtered_df, pd.DataFrame()


def korrigiere_windeinfluss(winkel_verbindungslinie_mp_io_windrichtung, windgeschwindigkeit):
    grenzwert = 0.5
    # eps:= Winkel zwischen Verbindungslinie IO_MP und Richtung, in die der Wind weht(180?? + Windrichtung)
    eps = winkel_verbindungslinie_mp_io_windrichtung
    if windgeschwindigkeit < grenzwert:
        return 0
    else:
        # Bemerkung: Maximale D??mpfung (von -10 dB) bei eps = 180
        # Minimale D??mpfung (von 0 dB) bei eps = 0
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
    # return angle  # im Bogenma??
    logging.debug(f"{angle} rad, {np.degrees(angle)} ??")
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
        f"""{mp_id}_{mp.column_lr}"""].rolling(20).apply(
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
    value_list = [(f"{e}_rechenwert", mp.Id) for mp in my_mps_data for e in mp.Ereignisse] # list(map(lambda j: f"""mp{j}_ohne_ereignis_rechenwert_auswertung""", ids_messpunkte))
    # rename_dict = dict(zip(key_list, [v[0] for v in value_list]))
    df_with_rechenwerte = df
    print(df_with_rechenwerte)
    for mp in my_mps_data:
        for e in mp.Ereignisse:
            if e == mp.column_lr:
                df_with_rechenwerte[f"{mp.Id}_{e}_rechenwert"] =  df[f"R{mp.Id}_LAFeq"]
            else:
                df_with_rechenwerte[f"{mp.Id}_{e}_rechenwert"] = 0

    print(df_with_rechenwerte)
    return df_with_rechenwerte




def werte_beurteilungszeitraum_aus(project: Projekt, zeitpunkt_im_zielzeitraum: datetime, has_mete = False, lafeq_gw=90, lafmax_gw = 100, use_terz_data = True):
    # my_ios = [1, 5, 9, 15, 17]
    if True:
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

        print(my_ios, my_mps_data, alle_verursacher)


    if True:

        my_ergebnis = erstelle_ergebnis_df(alle_verursacher, year, month, day, seconds_start, seconds_end, 1, my_mps_data)

        try:
            logging.info("Loading data...")
            if use_terz_data:
                terz = get_terz_all_mps(my_mps_data, from_date, to_date)
            resu = get_resu_all_mps(my_mps_data, from_date, to_date)



            
            if has_mete:
                mete = read_mete_data_v1(from_date, to_date)
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
            print(data_as_one)
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
                        modifizierte_pegel_wegen_grillen.index, f"""{mp.Id}_{mp.column_lr}"""]\
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
                    e = "vorbeifahrt"
                    vorbeifahrten_an_mp = find_vorbeifahrt_mp_5_immendingen(mp, df_with_rechenwert)
                    for i in vorbeifahrten_an_mp:
                        erkennung_set.append(i)

                    
                    vorbeifahrten_an_mp_indicator_series = \
                        create_indicator_vorbeifahrt(
                            df_with_rechenwert, vorbeifahrten_an_mp
                        )
                    df_with_rechenwert[f"{mp.Id}_{e}"] = df_with_rechenwert[f"{mp.Id}_{mp.column_lr}"]
                    df_with_rechenwert.loc[vorbeifahrten_an_mp_indicator_series, f"{mp.Id}_{mp.column_lr}"] = 0
                    df_with_rechenwert.loc[-vorbeifahrten_an_mp_indicator_series, f"{mp.Id}_{e}"] = 0
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


def berechne_lr_pegel_an_io(df_rechenwerte):
    df_rechenwerte_five_minute_vals = df_rechenwerte.cumsum().resample('5min', label='right').max()
    from_date = datetime(2022, 10, 17, 6, 0, 0)
    to_date = datetime(2022, 10, 17, 21, 59, 59)
    dti = pd.date_range(from_date + timedelta(seconds=299), to_date, freq="300s", name="Timestamp")
    print(df_rechenwerte_five_minute_vals)


def foo_immendingen_vorbeifahrt_erkennung(from_date, to_date, pegel_col: pd.Series, name_vorbeifahrt_col):
    total_number_seconds = int((to_date-from_date).total_seconds()) + 1
    rand_arr = (np.random.rand(total_number_seconds) - 0.75) >= 0

    print(len(rand_arr[rand_arr]))
    dti2 = pd.date_range(from_date, to_date, freq="1s", name="Timestamp")
    df2 = pd.Series(data = rand_arr, index=dti2)
    print(df2)

    p1 = pegel_col.copy(deep=True)
    p2 = pegel_col.copy(deep=True)

    p1.loc[df2] = 0
    p2.loc[~df2] = 0
    p2.name = name_vorbeifahrt_col

    return p1, p2



def berechne_schallleistungspegel_an_mp(mp: Messpunkt, pegel_an_mp_df: pd.DataFrame):
    schallleistungspegel = berechne_schallleistungpegel_an_mp_12_21(
                mp.Id, pegel_an_mp_df, mp.LWA
            )
    logging.info(f"Schallleistungspegel: {schallleistungspegel}")
    return schallleistungspegel

def berechne_max_pegel_an_io(io: Immissionsort, lafmax_pegel_an_mps: pd.DataFrame, wind_data_df: pd.DataFrame,mps, abf_data):
    cols_lautstaerke_von_verursacher = []


    
    for mp in mps:
        cols_lautstaerke_von_verursacher.append(berechne_hoechste_lautstaerke_an_io_12_21(io, mp, abf_data[(io.Id, mp.Id)], lafmax_pegel_an_mps, has_mete=wind_data_df is not None))

    df_lauteste_stunde_io_von_mp = pd.concat(cols_lautstaerke_von_verursacher, axis=1)
    arg_max_index_lautstaerke_io: datetime = df_lauteste_stunde_io_von_mp.max(axis=1).idxmax()
    arg_max_column_lautstaerke_io: str =df_lauteste_stunde_io_von_mp.loc[arg_max_index_lautstaerke_io, :].idxmax()

    logging.info(f"Lautesteter Pegel: {arg_max_index_lautstaerke_io}, {arg_max_column_lautstaerke_io}, {df_lauteste_stunde_io_von_mp.loc[arg_max_index_lautstaerke_io, arg_max_column_lautstaerke_io]}")

    return arg_max_index_lautstaerke_io, arg_max_column_lautstaerke_io, df_lauteste_stunde_io_von_mp.loc[arg_max_index_lautstaerke_io, arg_max_column_lautstaerke_io]



def berechne_pegel_an_io(from_date, to_date, io: Immissionsort, laerm_nach_ursachen_an_mps_df: pd.DataFrame, wind_data_df: pd.DataFrame, dict_abf_io_ereignis, rechenwert_verwertbare_sekunden):
    cols_laerm_nach_ursachen_an_io = []
    for col in laerm_nach_ursachen_an_mps_df.columns:
        if False: # wind_data_df is not None:
            winkel_io_mp = berechne_winkel_io_mp_12_21(Koordinaten(0, 0), Koordinaten(10, 0))
            result = wind_data_df.apply(lambda i: korrigiere_windeinfluss(winkel_io_mp + 180 + i["Windrichtung"], i["MaxWindgeschwindigkeit"]), axis=1)
            laerm_nach_ursachen_an_io_series: pd.DataFrame = laerm_nach_ursachen_an_mps_df[col] + result[laerm_nach_ursachen_an_mps_df.index] + dict_abf_io_ereignis[io.Id, col]
            print("laerm_nach_ursachen_an_io_df", laerm_nach_ursachen_an_io_series, laerm_nach_ursachen_an_mps_df[col])
            cols_laerm_nach_ursachen_an_io.append(laerm_nach_ursachen_an_io_series)
        else:
            laerm_nach_ursachen_an_io_series: pd.DataFrame = laerm_nach_ursachen_an_mps_df[col] + dict_abf_io_ereignis[io.Id, col]
            cols_laerm_nach_ursachen_an_io.append(laerm_nach_ursachen_an_io_series)
        laerm_nach_ursachen_an_io_series.name = col
        

    laerm_nach_ursachen_an_io_df = pd.concat(cols_laerm_nach_ursachen_an_io, axis=1)    
    dti3 = pd.date_range(from_date, to_date, freq="1s", name="Timestamp")
    df3 = pd.DataFrame(index=dti3)

    df_filled_holes = df3.merge(10**(0.1*laerm_nach_ursachen_an_io_df) / rechenwert_verwertbare_sekunden, how='left', left_index=True, right_index=True)
    df_filled_holes.fillna(0, inplace=True)
    

    

    dti = pd.date_range(from_date + timedelta(seconds=299), to_date, freq="300s", name="Timestamp")
    df1 = pd.DataFrame(index=dti)


    
    cumsummed_gesamt = 10 * np.log10(df_filled_holes.sum(axis=1).cumsum())
    cumsummed_gesamt.name = "Gesamt"
    df_gesamt_lr = df1.merge(cumsummed_gesamt,
                   how='left', left_index=True, right_index=True)

    df_all = df1.merge(10 * np.log10(df_filled_holes.cumsum()),
                   how='left', left_index=True, right_index=True)

    
    result_df = pd.merge(df_all, df_gesamt_lr, left_index=True, right_index=True)
    print(result_df)
    return result_df

def create_laermursachen_df(all_data_df: pd.DataFrame, mps: list[Messpunkt], from_date: datetime, to_date: datetime):
        cols_laerm = []
        for mp in mps:
            print(mp.Bezeichnung)
            if len(mp.Ereignisse) == 1:
                ereignis = [e for e in mp.Ereignisse if "Unkategorisiert" in e][0]
                s1 = create_rechenwert_column(all_data_df[f"R{mp.Id}_LAFeq"], ereignis)
                cols_laerm.append(s1)
            elif len(mp.Ereignisse) == 2:
                ereignis = [e for e in mp.Ereignisse if "Unkategorisiert" in e][0]
                ereignis_vorbeifahrt = [e for e in mp.Ereignisse if "Unkategorisiert" not in e][0]
                s1 = create_rechenwert_column(all_data_df[f"R{mp.Id}_LAFeq"], ereignis)
                s_ohne_vorbeifahrt, s_mit_vorbeifahrt = foo_immendingen_vorbeifahrt_erkennung(from_date, to_date, s1, ereignis_vorbeifahrt)

                cols_laerm.append(s_ohne_vorbeifahrt)
                cols_laerm.append(s_mit_vorbeifahrt)


        all_ursachen_df = pd.concat(cols_laerm, axis=1)
        print(all_ursachen_df)
        return all_ursachen_df

def foo_mannheim_zug_erkennung():
    pass

def create_rechenwert_column(column: pd.Series, new_name: str):
    c = column.copy(deep=True)
    c.name = new_name
    return c
    


def load_data(from_date, to_date, my_mps_data, use_terz_data=True, has_mete=True):
    if use_terz_data:
        terz = get_terz_all_mps(my_mps_data, from_date, to_date)
        resu = get_resu_all_mps(my_mps_data, from_date, to_date)

    if has_mete:
        mete = read_mete_data_v1(from_date, to_date)
        if use_terz_data:
            data_as_one = create_complete_df(resu, terz, mete, has_mete)
        else:
            data_as_one = create_complete_df(resu, [], mete, has_mete, False)
    else:
        if use_terz_data:
            data_as_one = create_complete_df(resu, terz, [], has_mete)
        else:
            data_as_one = create_complete_df(resu, [], [], has_mete, False)
    print(data_as_one)
    return data_as_one


def foo(from_date, to_date, ursachen_an_mps):
    
    dti = pd.date_range(from_date + timedelta(seconds=299), to_date, freq="300s", name="Timestamp")
    df1 = pd.DataFrame(index=dti)
    print(df1)
    total_number_seconds =(to_date-from_date).total_seconds() + 1
    print(int(total_number_seconds))

    dti2 = pd.date_range(from_date, to_date, freq="1s", name="Timestamp")

    d = dict(zip(ursachen_an_mps, [15*np.random.rand(int(total_number_seconds)) + 60 for mp in ursachen_an_mps]))
    # print(d[ursachen_an_mps[0]])
    df2 = pd.DataFrame(data = d, index=dti2)

    df2 = df2.loc[df2[ursachen_an_mps[0]] >= 70]

    print(df2)

    dti3 = pd.date_range(from_date, to_date, freq="1s", name="Timestamp")
    df3 = pd.DataFrame(index=dti3)

    df_filled_holes = df3.merge(df2,
                   how='left', left_index=True, right_index=True)

    df_filled_holes.fillna(0, inplace=True)


    df_all = df1.merge(df_filled_holes.cumsum(),
                   how='left', indicator=True, left_index=True, right_index=True)

    print(df_all)





def filter_and_modify_data(my_mps_data: list[Messpunkt], all_data_df: pd.DataFrame, has_mete: bool):
    lafeq_gw = 90
    lafmax_gw = 100
    lcfeq_gw = 110
    use_terz_data = True

    messwerte_nach_filtern_df = all_data_df
    logging.debug(f"Vor Filtern {len(messwerte_nach_filtern_df)}")
    s1 = pd.Series(index=all_data_df.index, dtype="string")
    s2 = pd.Series(index=all_data_df.index, dtype="int")
    filter_result_df = pd.DataFrame(data={"ursache": s1, "messpunkt_id": s2})
    print(filter_result_df)
    if has_mete:
        ausortiert_by_windfilter = filter_wind_12_21(messwerte_nach_filtern_df)
        s1.loc[ausortiert_by_windfilter[ausortiert_by_windfilter].index] = "wind"
        filter_result_df.loc[ausortiert_by_windfilter[ausortiert_by_windfilter].index, :] = ["wind", my_mps_data[0].id_in_db]
        
        messwerte_nach_filtern_df = messwerte_nach_filtern_df[-ausortiert_by_windfilter]


        logging.debug(f"Nach Windfilter: {len(messwerte_nach_filtern_df)}")
        ausortiert_by_regen = filter_regen_12_21(messwerte_nach_filtern_df)
        messwerte_nach_filtern_df = messwerte_nach_filtern_df[-ausortiert_by_regen]
        s1.loc[ausortiert_by_regen[ausortiert_by_regen].index] = "regen"
        filter_result_df.loc[ausortiert_by_regen[ausortiert_by_regen].index, :] = ["regen", my_mps_data[0].id_in_db]

        # my_results_filter["regen"] = [] #ausortiert_by_regen[ausortiert_by_regen]
        logging.debug(f"Nach Regenfilter: {len(messwerte_nach_filtern_df)}")

    print("Filter-Zwischenergebnisse:", filter_result_df)
    
    if True:
        for mp in my_mps_data:
            aussortiert_by_simple_filter = simple_filter_mp_column_in_measurement_data_12_21(mp.Id, "LAFeq", lafeq_gw, messwerte_nach_filtern_df)
            messwerte_nach_filtern_df = messwerte_nach_filtern_df[-aussortiert_by_simple_filter]
            s1.loc[aussortiert_by_simple_filter[aussortiert_by_simple_filter].index] = "lafeq"
            filter_result_df.loc[aussortiert_by_simple_filter[aussortiert_by_simple_filter].index, :] = ["lafeq", mp.id_in_db]
            aussortiert_by_simple_filter = simple_filter_mp_column_in_measurement_data_12_21(mp.Id, "LAFmax", lafmax_gw, messwerte_nach_filtern_df)
            messwerte_nach_filtern_df = messwerte_nach_filtern_df[-aussortiert_by_simple_filter]
            s1.loc[aussortiert_by_simple_filter[aussortiert_by_simple_filter].index] = "lafmax"
            filter_result_df.loc[aussortiert_by_simple_filter[aussortiert_by_simple_filter].index, :] = ["lafmax", mp.id_in_db]
            if False:
                if "Zug" in mp.Filter:
                    logging.info(f"Vor Zugfilter: {len(messwerte_nach_filtern_df)}")
                    if False:
                        aussortiert_by_zugfilter = filter_zug(mp.Id, messwerte_nach_filtern_df)
                        messwerte_nach_filtern_df = messwerte_nach_filtern_df[-aussortiert_by_zugfilter]
                        print(f"Nach Zugfilter: {len(messwerte_nach_filtern_df)}")
                        aussortierung_set.append(Aussortiert(aussortiert_by_zugfilter[aussortiert_by_zugfilter], "zug", mp))
                    if False:

                        detections = filter_zug_v2(mp.Id, messwerte_nach_filtern_df)
                        for d in detections:
                            d: Detected
                            aussortierung_set.append(Aussortiert(messwerte_nach_filtern_df.loc[(messwerte_nach_filtern_df.index >= d.start) & (messwerte_nach_filtern_df.index <= d.end)].index.to_series(), "Zug_V2", mp))
                            messwerte_nach_filtern_df = messwerte_nach_filtern_df.loc[(messwerte_nach_filtern_df.index < d.start) | (messwerte_nach_filtern_df.index > d.end)]
                        
                    if True:
                        detections = filter_zug_v3(mp.Id, messwerte_nach_filtern_df)
                        for d in detections:
                            d: Detected
                            messwerte_nach_filtern_df = messwerte_nach_filtern_df.loc[(messwerte_nach_filtern_df.index < d.start) | (messwerte_nach_filtern_df.index > d.end)]
                    logging.info(f"Nach Zugfilter: {len(messwerte_nach_filtern_df)}")
            if use_terz_data:
                aussortiert_by_vogelfilter = filter_vogel_12_21(mp.Id, messwerte_nach_filtern_df)
                logging.debug(f"aussortiert_by_vogelfilter {aussortiert_by_vogelfilter}")
                messwerte_nach_filtern_df = messwerte_nach_filtern_df[aussortiert_by_vogelfilter]
                s1.loc[aussortiert_by_vogelfilter[-aussortiert_by_vogelfilter].index] = "vogel"
                filter_result_df.loc[aussortiert_by_vogelfilter[aussortiert_by_vogelfilter].index, :] = ["vogel", mp.id_in_db]
                # my_results_filter[f"vogelMp{mp.id}"] = [] # aussortiert_by_vogelfilter[-aussortiert_by_vogelfilter]
                if True:
                    modifizierte_pegel_wegen_grillen = find_and_modify_grillen(mp.Id, messwerte_nach_filtern_df)
                    s1.loc[modifizierte_pegel_wegen_grillen[modifizierte_pegel_wegen_grillen].index] = "grille"
                    filter_result_df.loc[modifizierte_pegel_wegen_grillen[modifizierte_pegel_wegen_grillen].index, :] = ["grille", mp.id_in_db]
                    if True:
                        messwerte_nach_filtern_df.loc[
                            modifizierte_pegel_wegen_grillen.index, f"""R{mp.Id}_LAFeq"""]\
                            = modifizierte_pegel_wegen_grillen
                        logging.debug(modifizierte_pegel_wegen_grillen)
    logging.debug(f'Aussortierte Sekunden: {s1[s1 != "<NA>"]}')
    print("Filter-Ergebnisse:", filter_result_df)
    aussortierte_sekunden_mit_grund_df = filter_result_df.dropna()

    return messwerte_nach_filtern_df, aussortierte_sekunden_mit_grund_df


def get_project_via_rest() -> Projekt:
    p = requests.get("http://localhost:8000/tsdb/projekt/")
    p.raise_for_status()
    projekt_json = p.json()
    
    idx = 0

    abfs_json = projekt_json[idx]["ausbreitungsfaktoren_set"]

    dict_abf_io_ereignis = {}
    

    abfs = dict(zip([(a["immissionsort"], a["messpunkt"]) for a in abfs_json], [a["ausbreitungskorrektur"] for a in abfs_json]))
        

    mps = [Messpunkt(mp_json['id_external'], Bezeichnung=mp_json['name'], Koordinaten=Koordinaten(mp_json["gk_rechts"], mp_json["gk_hoch"]), Ereignisse=[e["name"] for e in mp_json["laermursacheanmesspunkt_set"]] ) for mp_json in projekt_json[idx]['messpunkt_set']]
    
    ios = [Immissionsort(io_json['id_external'], Bezeichnung=io_json["name"], Koordinaten=Koordinaten(io_json["gk_rechts"], io_json["gk_hoch"]), id_in_db=io_json["id"]) for io_json in projekt_json[idx]['immissionsort_set']]
    for mp in mps:
        mp: Messpunkt
        mp.column_lr = mp.Ereignisse[0]
        for e in mp.Ereignisse:
            for io in ios:
                dict_abf_io_ereignis[(io.Id, e)] = abfs[(io.Id, mp.Id)]

    ursachen_an_ios = dict(zip([el["name"] for el in projekt_json[idx]["laermursacheanimmissionsorten_set"]], projekt_json[idx]["laermursacheanimmissionsorten_set"])) 
    p1 = Projekt(projekt_json[idx]['name'], ios, mps, abfs, "blub", has_mete_data=True, dict_abf_io_ereignis = dict_abf_io_ereignis, id_in_db =  projekt_json[idx]["id"],ursachen_an_ios=ursachen_an_ios)
    return p1


def bestimme_rechenwert_verwertbare_sekunden(messwerte_nach_filtern_df):
    anzahl_verwertbare_sekunden, verwertbare_messwerte_df, aussortiert_wegen_sample_size = berechne_verwertbare_sekunden(messwerte_nach_filtern_df)
    # logging.debug(f"aussortiert_wegen_sample_size: {aussortiert_wegen_sample_size}")
    logging.debug(f"Verwertbare Sekunden (Rechenwert): {anzahl_verwertbare_sekunden}")

    return verwertbare_messwerte_df, anzahl_verwertbare_sekunden


def werte_beurteilungszeitraum_aus(datetime_in_beurteilungszeitraum: datetime):
    
    from_date,to_date = get_start_end_beurteilungszeitraum_from_datetime(datetime_in_beurteilungszeitraum)

    from_date_data_vorhanden = from_date + timedelta(seconds=5)
    to_date_data_vorhanden = to_date - timedelta(seconds=5)

    logging.info(f"{from_date}, {to_date}")    
    if True:
        
        
        p = get_project_via_rest()

        lrpegel_set = []
        maxpegel_set = []
        schallleistungspegel_set = []
        rejected_set = []
        detected_set = []

        all_data_df = load_data(from_date_data_vorhanden, to_date_data_vorhanden, p.MPs, True, True)

        number_seconds_with_all_measurements = len(all_data_df)

        filtered_and_modified_df, aussortierte_sekunden_mit_grund = filter_and_modify_data(p.MPs, all_data_df, True)

        for idx, row in aussortierte_sekunden_mit_grund.iterrows():
            rejected_set.append(
                 DTO_Rejected(idx, 1, row["messpunkt_id"])
            )

        verwertbare_messwerte_df, rechenwert_verwertbare_sekunden = bestimme_rechenwert_verwertbare_sekunden(filtered_and_modified_df)

        number_seconds_with_evaluatable_measurements = len(verwertbare_messwerte_df)

        if rechenwert_verwertbare_sekunden == 0:
            logging.info("Keine nutzbaren Messdaten vorhanden")

        else:
            logging.info("Erstelle Auswertung auf Basis der verwertbaren Messdaten")

            laermursachen_an_messpunkten = create_laermursachen_df(verwertbare_messwerte_df, p.MPs, from_date, to_date)

            

            for mp in p.MPs:
                schallleistungspegel = berechne_schallleistungspegel_an_mp(mp, verwertbare_messwerte_df)
                schallleistungspegel_set.append(
                 DTO_SchallleistungPegel(idx, 1, p.MPs[0].Id)
            )

            

            for io in p.IOs:
                zeitpunkt_maxpegel_an_io, ursache_maxpegel_an_io, maxpegel_an_io = berechne_max_pegel_an_io(io, verwertbare_messwerte_df, verwertbare_messwerte_df, p.MPs, p.Ausbreitungsfaktoren)
                print(ursache_maxpegel_an_io, zeitpunkt_maxpegel_an_io, maxpegel_an_io)
                maxpegel_set.append(
                    DTO_MaxPegel(zeitpunkt_maxpegel_an_io, maxpegel_an_io, io.id_in_db)
                )
                

                result_lr = berechne_pegel_an_io(from_date, to_date, io, laermursachen_an_messpunkten, verwertbare_messwerte_df, p.dict_abf_io_ereignis, rechenwert_verwertbare_sekunden)


                for col in result_lr.columns:
                    for idx, val in result_lr[col].items():
                        lrpegel_set.append(DTO_LrPegel(idx, val, p.ursachen_an_ios[col]["id"], io.id_in_db))
                    
    
    ergebnis = Ergebnisse(from_date, datetime.now(), 
    number_seconds_with_all_measurements, number_seconds_with_evaluatable_measurements, rechenwert_verwertbare_sekunden,
    detected_set, lrpegel_set, rejected_set, maxpegel_set, schallleistungspegel_set, p.id_in_db)

    return ergebnis


def make_json():
    pass
if __name__ == "__main__":
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
    level=logging.DEBUG, format=FORMAT, handlers=[
        #logging.FileHandler("eval.log"),
        logging.StreamHandler(sys.stdout)
        ]
    )



    


        


        