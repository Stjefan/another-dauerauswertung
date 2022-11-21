
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


