import argparse
import requests
import datetime as dt
import json
import pandas as pd
ios_immendingen = []
import logging


def create_rolling_csv(from_date: dt.datetime, to_date: dt.datetime):
    """
    To be updated
    """
    ID_PROJEKT = 1

    # heute = dt.datetime.now()
    # gewaehltes_datum =  dt.datetime(heute.year, heute.month, heute.day)
    # datum_7_days_before = gewaehltes_datum + dt.timedelta(days = -7)
    # 

    gewaehltes_datum = to_date
    datum_7_days_before = from_date
    day_before_gewaehltes_datum = gewaehltes_datum + dt.timedelta(days = -1)
    gesamtergebnis = []
    for io in ios_immendingen:
        id_immissionsort = io.Id
        for j in range(0, 9):
            id_beurteilungszeitraum = j
            for jj in range(1, 8):
                aktuelles_timedelta = dt.timedelta(jj)
                datum_aktuelle_abfrage = gewaehltes_datum - aktuelles_timedelta
                API = f"http://kuf-srv-02:8080/Auswertungsdaten/Ergebnisse?idProjekt={ID_PROJEKT}&idImmissionsort={id_immissionsort}&day={datum_aktuelle_abfrage.isoformat()}&idBeurteilungszeitraum={id_beurteilungszeitraum}"
                response = requests.get(API)
                ergebnisse = json.loads(response.content)
                for el in ergebnisse:
                    el["Timestamp"] = datum_aktuelle_abfrage + dt.timedelta(seconds = el["sekunde"])
                    gesamtergebnis.append(el)



    result = pd.DataFrame(data = gesamtergebnis)
    sekunden_teilbar_durch_900 = ((result["sekunde"] +1) % 900) == 0
    result_15min_interval = result[sekunden_teilbar_durch_900]
    result_15min_interval_dropped_cols = result_15min_interval.drop(["sekunde", "tag", "verursachtDurch", "auswertung"], 1)
    sorted_result = result_15min_interval_dropped_cols.sort_values(by=["Timestamp", "idImmissionsort"])

    sorted_result.rename({"idImmissionsort": "IdImmissionsort", "bewertungspegel": "Beurteilungspegel"}, axis = 1, inplace = True)

    sorted_result.set_index("Timestamp", inplace=True)
    sorted_result.to_csv(f"ios_immendingen_rolling_{datum_aktuelle_abfrage.strftime('%Y_%m_%d')}_To_{day_before_gewaehltes_datum.strftime('%Y_%m_%d')}.csv",decimal=",", sep=";", date_format="%d.%m.%Y %H:%M:%S")
