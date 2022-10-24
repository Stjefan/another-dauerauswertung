import logging
import sys
from auswertung_service import werte_beurteilungszeitraum_aus
from insert_auswertung_service import insert_auswertung_via_psycopg2
from datetime import datetime


if __name__ == "__main__":
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
    level=logging.DEBUG, format=FORMAT, handlers=[
        #logging.FileHandler("eval.log"),
        logging.StreamHandler(sys.stdout)
        ]
    )
    zeitpunkt = datetime(2022, 10, 24, 6, 30, 0)
    ergebnis = werte_beurteilungszeitraum_aus(zeitpunkt, "Mannheim")
    insert_auswertung_via_psycopg2(zeitpunkt, ergebnis)
