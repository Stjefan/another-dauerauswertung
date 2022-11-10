
from dataclasses import dataclass, field
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple
import psycopg2
from calendar import monthrange
from DTO import Immissionsort, Projekt

import pandas as pd

DataFrame = pd.DataFrame
@dataclass
class MonatsuebersichtAnImmissionsort:
    immissionsort: Immissionsort
    lr_tag: DataFrame = None
    lr_max_nacht: DataFrame = None
    lauteste_stunde_tag:  DataFrame = None
    lauteste_stunde_nacht: DataFrame = None
    
@dataclass
class MonatsuebersichtAnImmissionsortV2:
    immissionsort: Immissionsort = None
    lr_tag: Dict[datetime, float] = field(default_factory=dict)
    lr_max_nacht: Dict[datetime, Tuple[float, int]] = field(default_factory=dict)
    lauteste_stunde_tag:  Dict[datetime, float] = field(default_factory=dict)
    lauteste_stunde_nacht: Dict[datetime, Tuple[float, int]] = field(default_factory=dict)


@dataclass
class Monatsbericht:
    monat: datetime
    projekt: Projekt
    no_verwertbare_sekunden: int
    no_aussortiert_wetter: int
    no_aussortiert_sonstige: int
    ueberschrift: str
    details_io: Dict[int, MonatsuebersichtAnImmissionsort]
    schallleistungspegel: Dict[Tuple[int, int], float] = None
    
if True:
    conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5432/tsdb")
    cursor = conn.cursor()
    current_time = datetime.now() + timedelta(hours=-44)

    projekt_id = 2
    q_tz = """SET TIME ZONE 'Europe/Rome'"""
    after_time =   datetime(current_time.year, current_time.month, current_time.day, 6, 0, 0)
    before_time = after_time+ timedelta(days=2)
    
    q_day = f"""
    SELECT time::date, max(pegel) FROM tsdb_lrpegel lr WHERE time >= '{after_time}' AND time <= '{before_time}' GROUP BY time::date;
    """

    q_night = f"""
    SELECT * FROM (
        SELECT time::date AS time, date_part( 'hour', time), max(pegel) AS pegel FROM tsdb_lrpegel lr WHERE time >= '{after_time}' AND time <= '{before_time}' AND time::time <= '06:00' GROUP BY time::date, date_part('hour', time)) T1
    JOIN tsdb_lrpegel T2 On T1.time = T2.time::date and T1.pegel = T2.pegel;
    """

    q_filtered = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id = 1"""


    q_wetter_filter = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id = 1 AND (rej.filter_id = 4 or rej.filter_id = 5)"""
    q_sonstige_filter = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id = 1 AND rej.filter_id != 4 and rej.filter_id != 5"""

    q_schalllesitungspegel = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id = 1"""

    project_id = 1
    q_verfuegbare_sekunden = f"""SELECT sum(verwertebare_messwerte) FROM tsdb_auswertungslauf WHERE zeitpunkt_im_beurteilungszeitraum >= '{after_time}' AND zeitpunkt_im_beurteilungszeitraum <= '{before_time}' AND zuordnung_id = {project_id};"""

    q_schallleistungpegel = f"""SELECT * FROM tsdb_schallleistungpegel WHERE time >= '{after_time}' AND time <= '{before_time}' AND messpunkt_id = 4;"""


    q_arg_max = """SELECT x.*
  FROM (SELECT y.*,
               ROW_NUMBER() OVER (ORDER BY y.pegel DESC) AS rank
          FROM tsdb_lrpegel y WHERE time >= '2022-11-09' and time <= '2022-11-10' and immissionsort_id = 5) x
 WHERE x.rank = 1;
 """
    cursor.execute(q_tz)
    results = cursor.execute(q_verfuegbare_sekunden)
    print(cursor.fetchall())

    results = cursor.execute(q_schallleistungpegel)
    print(cursor.fetchall())
    conn.close()



def read_data_for_monatsbericht(project, year: int, month: int, has_mete: bool = False, read_schallleistung = False):
    # project = project_immendingen
    project_name = project.name_in_db
    ios = project.IOs
    number_days_in_month = monthrange(year, month)[1]
    range_start = datetime(year,month,1, 0, 0, 0)
    range_stop = datetime(year,month,1, 0, 0, 0) + timedelta(days=number_days_in_month)

    io_monatsuebersicht: typing.Dict[int, MonatsuebersichtAnImmissionsort] = dict(zip([io.Id for io in project.IOs], [MonatsuebersichtAnImmissionsort(io) for io in ios]))
    
    number_days_in_month = monthrange(range_start.year, range_start.month)[1]
    logging.info(f"Seconds in month: {number_days_in_month*3600*24}")
    with InfluxDBClient(url=influx_url, token=token, org=org, timeout=1000*600) as client:
        if read_schallleistung:
            schallleistungspegel = {}
            df = read_schallleistungspegel(range_start, range_stop, project.name_in_db)
            for idx, row in df.iterrows():
                schallleistungspegel[idx] = row["schallleistungspegel"]
                print(idx, row["schallleistungspegel"])
                

            
        else:
            schallleistungspegel = None

        query_verfuegbare_sekunden = f"""from(bucket: "dauerauswertung_immendingen")
            |> range(start: {range_start.strftime(ISOFORMAT)}, stop: {range_stop.strftime(ISOFORMAT)})
            |> filter(fn: (r) => r["_measurement"] == "auswertung_{project_name}_auswertungslauf" and r["_field"] == "verfuegbare_sekunden")
            |> group(columns: ["_measurement"])
            |> sum(column: "_value")"""
        df_verfuegbare_sekunden = client.query_api().query_data_frame(query_verfuegbare_sekunden)
        logging.info(f"verfuegbare_sekunden: {df_verfuegbare_sekunden['_value'][0]}")
        logging.info(type(df_verfuegbare_sekunden['_value'][0]))
        query = f'''from(bucket: "dauerauswertung_immendingen")
                |> range(start: {range_start.strftime(ISOFORMAT)}, stop: {range_stop.strftime(ISOFORMAT)})
                |> filter(fn: (r) => r["_measurement"] == "auswertung_{project_name}_lr" and r["_field"] == "lr" and r["verursacher"] == "gesamt")'''
        query_lauteste_stunde = f'''from(bucket: "dauerauswertung_immendingen")
        |> range(start: {range_start.strftime(ISOFORMAT)}, stop: {range_stop.strftime(ISOFORMAT)})
            |> filter(fn: (r) => r["_measurement"] == "auswertung_{project_name}_lauteste_stunde")
            |> filter(fn: (r) => r["_field"] == "lauteste_stunde")'''
        df : pd.DataFrame = client.query_api().query_data_frame(query)
        df.drop(["table", "_start", "_stop", "_measurement", "_field", "result", "verursacher"], axis=1, inplace=True)
        df = df.astype({'immissionsort': 'int32'})

        df_nachts = df[df["_time"].dt.hour.isin([0, 1, 2, 3, 4, 5,  22, 23])]
        df_tagzeitraum = df[~df["_time"].dt.hour.isin([0, 1, 2, 3, 4, 5,  22, 23])]
        
        for df in [df_tagzeitraum]:
            for io in ios:

                df_an_io = df[df["immissionsort"] == io.Id].set_index(["_time"])
                lr = df_an_io.groupby(by= lambda idx: idx.day).max()

                io_monatsuebersicht[io.Id].lr_tag = lr
                    
        df_tagzeitraum.set_index(["_time", "immissionsort"], inplace=True)
        
        df_nachts["day"] = df_nachts["_time"].dt.day
        df_lr_nachts = df_nachts.sort_values('_value').drop_duplicates(["day", "immissionsort"],keep='last')
        df_lr_nachts.sort_values(["immissionsort", "day"], inplace=True)
        for io in ios: 
            df_lr_nachts_an_io = df_lr_nachts[df_lr_nachts["immissionsort"] == io.Id].loc[:, ["_time", "_value", "day"]]
            df_lr_nachts_an_io = df_lr_nachts_an_io.set_index(["day"])
            io_monatsuebersicht[io.Id].lr_max_nacht = df_lr_nachts_an_io

        df_lauteste_stunde = client.query_api().query_data_frame(query_lauteste_stunde)
        df_lauteste_stunde.drop(["table", "_start", "_stop", "_measurement", "_field", "result"], axis=1, inplace=True)
        df_lauteste_stunde_nachts = df_lauteste_stunde[df_lauteste_stunde["_time"].dt.hour.isin([0, 1, 2, 3, 4, 5,  22, 23])]
        df_lauteste_stunde_tagzeitraum = df_lauteste_stunde[~df_lauteste_stunde["_time"].dt.hour.isin([0, 1, 2, 3, 4, 5,  22, 23])]
        df_lauteste_stunde_tagzeitraum.loc[:, "day"] = df_lauteste_stunde_tagzeitraum["_time"].dt.day
        df_lauteste_stunde_nachts.loc[:, "day"] = df_lauteste_stunde_nachts["_time"].dt.day
        df_lauteste_stunde_tagzeitraum = df_lauteste_stunde_tagzeitraum.sort_values('_value').drop_duplicates(["day", "immissionsort"],keep='last')
        df_lauteste_stunde_tagzeitraum.sort_values(["immissionsort", "day"], inplace=True)

        df_lauteste_stunde_nachts = df_lauteste_stunde_nachts.sort_values('_value').drop_duplicates(["day", "immissionsort"],keep='last')
        df_lauteste_stunde_nachts.sort_values(["immissionsort", "day"], inplace=True)

        for io in ios: 
            io_monatsuebersicht[io.Id].lauteste_stunde_tag = df_lauteste_stunde_tagzeitraum[df_lauteste_stunde_tagzeitraum["immissionsort"] == io.Bezeichnung].loc[:, ["day", "_value", "_time"]].set_index(["day"])
            io_monatsuebersicht[io.Id].lauteste_stunde_nacht = df_lauteste_stunde_nachts[df_lauteste_stunde_nachts["immissionsort"] == io.Bezeichnung].loc[:, ["day", "_value", "_time"]].set_index(["day"])
        
        if has_mete:
            q_aussortiert_wetter = f"""from(bucket: "dauerauswertung_immendingen")
                |> range(start: {range_start.strftime(ISOFORMAT)}, stop: {range_stop.strftime(ISOFORMAT)})
                |> filter(fn: (r) => r["_measurement"] == "auswertung_{project_name}_aussortierung" and contains(value: r["_value"], set: ["wind", "regen"]))
                |> group(columns: ["_measurement"])
                |> count()"""
            df_aussortiert_wetter = client.query_api().query_data_frame(q_aussortiert_wetter)
            if not df_aussortiert_wetter.empty:
                logging.info(f"q_aussortiert_wetter: {df_aussortiert_wetter['_value'][0]}")
                wegen_wetter_aussortiert = df_aussortiert_wetter['_value'][0]
            else:
                wegen_wetter_aussortiert = 0
        else:
            wegen_wetter_aussortiert= 0

        q_aussortiert_gesamt = f"""from(bucket: "dauerauswertung_immendingen")
            |> range(start: {range_start.strftime(ISOFORMAT)}, stop: {range_stop.strftime(ISOFORMAT)})
            |> filter(fn: (r) => r["_measurement"] == "auswertung_{project_name}_aussortierung") |> group(columns: ["_measurement"])
            |> count()"""
        df_aussortiert_gesamt = client.query_api().query_data_frame(q_aussortiert_gesamt)
        logging.info(f"q_aussortiert_gesamt: {df_aussortiert_gesamt['_value'][0]}")
        q_aussortiert_sonstiges = f"""from(bucket: "dauerauswertung_immendingen")
            |> range(start: {range_start.strftime(ISOFORMAT)}, stop: {range_stop.strftime(ISOFORMAT)})
            |> filter(fn: (r) => r["_measurement"] == "auswertung_{project_name}_aussortierung" and not contains(value: r["_value"], set: ["wind", "regen"])) |> group(columns: ["_measurement"])
            |> count()"""
        df_aussortiert_sonstiges = client.query_api().query_data_frame(q_aussortiert_sonstiges)

        logging.info(f"q_aussortiert_sonstiges: {df_aussortiert_sonstiges['_value'][0]}")

        a = Monatsbericht(range_start, project,
                        df_verfuegbare_sekunden['_value'][0], wegen_wetter_aussortiert,
                        df_aussortiert_sonstiges['_value'][0], "blub", io_monatsuebersicht, schallleistungspegel)
        return a
