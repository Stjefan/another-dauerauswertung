from calendar import monthrange
import logging
import sys
from auswertung_service import werte_beurteilungszeitraum_aus
from insert_auswertung_service import insert_auswertung_via_psycopg2
from datetime import datetime


if __name__ == "__main__":
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
    level=logging.INFO, format=FORMAT, handlers=[
        logging.FileHandler("../logs/dauerauswertung.log"),
        logging.StreamHandler(sys.stdout)
        ]
    )
    year = 2022
    month = 10
    
    fun_2_run_1 = werte_beurteilungszeitraum_aus
    fun_2_run_2 = insert_auswertung_via_psycopg2
    projektbezeichnung = "mannheim"
    if True:
        for d in range(20, min(26, monthrange(year, month)[1])+1):
            for h in [0, 1, 2, 3, 4, 5, 21, 22, 23]:
                try:
                    zeitpunkt = datetime(year, month, d, h, 30, 0)
                    ergebnis = fun_2_run_1(zeitpunkt, projektbezeichnung)
                    fun_2_run_2(zeitpunkt, ergebnis)
                except Exception as e:
                    logging.info(f"At {year} {month} {d} {h}")
                    logging.exception(e)

    if False:
        try:
            zeitpunkt = datetime(year, month, 25, 3, 30, 0)
            ergebnis = fun_2_run_1(zeitpunkt, projektbezeichnung)
            fun_2_run_2(zeitpunkt, ergebnis)
        except Exception as e:
            logging.exception(e)



