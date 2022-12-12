import argparse
from calendar import monthrange
import logging
import sys
from time import sleep
from auswertung_service import werte_beurteilungszeitraum_aus
from insert_auswertung_service import insert_auswertung_via_psycopg2
from datetime import datetime, timedelta



if __name__ == "__main__":
    today = datetime.now()
    parser = argparse.ArgumentParser(description='Führt Berechnungen aus')
    parser.add_argument('--modus', type=str, required=False, choices=["single", "monat", "tag", "dauernd"],
                        help='Ort, an dem die Output-xlsx gespeichert wird')
    parser.add_argument('--monat', type=int, required=False, default=today.month,
                        help='Pfad zur json-Datei mit den Monatsberichtsdaten')

    parser.add_argument('--jahr', type=int, required=False, default=today.year,
                        help='Ort, an dem die Output-xlsx gespeichert wird')
    parser.add_argument('--tag', type=int, required=False, default=today.day,
                        help='Ort, an dem die Output-xlsx gespeichert wird')
    parser.add_argument('--stunde', type=int, required=False, default=today.hour,
                        help='Ort, an dem die Output-xlsx gespeichert wird')

    parser.add_argument('--projektbezeichnung', type=str, required=False, default="immendingen", choices=["immendingen", "mannheim"],
                        help='Ort, an dem die Output-xlsx gespeichert wird')

    args = parser.parse_args()
    print(args.modus)
    print(args.monat)
    print(args.jahr)
        
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
    level=logging.INFO, format=FORMAT, handlers=[
        logging.FileHandler(f"../logs/dauerauswertung_{args.modus}.log"),
        logging.StreamHandler(sys.stdout)
        ]
    )
    year = args.jahr
    month = args.monat
    day = args.tag
    stunde = args.stunde
    
    fun_2_run_1 = werte_beurteilungszeitraum_aus
    fun_2_run_2 = insert_auswertung_via_psycopg2
    projektbezeichnung = args.projektbezeichnung # "immendingen"
    if args.modus == 'monat':
        for d in range(1, min(31, monthrange(year, month)[1])+1):
            for h in [0, 1, 2, 3, 4, 5, 21, 22, 23]:
                try:
                    zeitpunkt = datetime(year, month, d, h, 30, 0)
                    ergebnis = fun_2_run_1(zeitpunkt, projektbezeichnung)
                    fun_2_run_2(zeitpunkt, ergebnis)
                except Exception as e:
                    logging.info(f"At {year} {month} {d} {h}")
                    logging.exception(e)

    if args.modus in ['single']:
        try:
            zeitpunkt = datetime(year, month, day, stunde, 30, 0)
            ergebnis = fun_2_run_1(zeitpunkt, projektbezeichnung)
            fun_2_run_2(zeitpunkt, ergebnis)
        except Exception as e:
            logging.exception(e)
    if args.modus in ['tag']:
        for h in [0, 1, 2, 3, 4, 5, 21, 22, 23]:
                try:
                    zeitpunkt = datetime(year, month, day, h, 30, 0)
                    ergebnis = fun_2_run_1(zeitpunkt, projektbezeichnung)
                    fun_2_run_2(zeitpunkt, ergebnis)
                except Exception as e:
                    logging.info(f"At {year} {month} {d} {h}")
                    logging.exception(e)
    if args.modus in ['dauernd', None]:
        iteration_counter = 0
        while True:
            iteration_counter += 1
            
            for name in ["immendingen", "mannheim"]:
                try:
                    current_moment = datetime.now()
                    logging.info(f"Iteraiton {iteration_counter} with {current_moment} and {name}")
                    projektbezeichnung = name
                    zeitpunkt = current_moment + timedelta(seconds=-300)
                    ergebnis = fun_2_run_1(zeitpunkt, projektbezeichnung)
                    fun_2_run_2(zeitpunkt, ergebnis)
                except Exception as e:
                    logging.exception(e)
            logging.info(f"Going to sleep")
            sleep(300)





