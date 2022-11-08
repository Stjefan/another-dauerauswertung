
import logging
from datetime import datetime, timedelta
import psycopg2
if True:
    conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5432/tsdb")
    cursor = conn.cursor()
    current_time = datetime.now() + timedelta(hours=-44)

    projekt_id = 2
    q_tz = """SET TIME ZONE 'Europe/Rome'"""
    if 6 <= current_time.hour <= 21:
        after_time =   datetime(current_time.year, current_time.month, current_time.day, 6, 0, 0)
        before_time = after_time+ timedelta(hours=16)
        
        q = f"""
        SELECT immissionsort_id, max(pegel) FROM tsdb_lrpegel lr JOIN tsdb_immissionsort io ON lr.immissionsort_id = io.id AND io.projekt_id = {projekt_id} WHERE time >= '{after_time}' AND time <= '{before_time}' AND lr.pegel >= 0.5*io.grenzwert_tag GROUP BY lr.immissionsort_id;
        """
    else:
        after_time =  datetime(current_time.year, current_time.month, current_time.day, current_time.hour, 0, 0)
        before_time = after_time+ timedelta(hours=1)
        q = f"""
        SELECT immissionsort_id, max(pegel) FROM tsdb_lrpegel lr JOIN tsdb_immissionsort io ON lr.immissionsort_id = io.id AND io.projekt_id = {projekt_id} WHERE time >= '{after_time}' AND time <= '{before_time}' AND lr.pegel >= 0.5*io.grenzwert_nacht GROUP BY lr.immissionsort_id;
        """
    cursor.execute(q_tz)
    results = cursor.execute(q)
    print(cursor.fetchall())
    conn.close()
        

from time import sleep

from math import log10
from datetime import datetime, timedelta

import logging
import sys

import logging
import logging.config

import smtplib
from email.message import EmailMessage
from email.utils import formatdate
from datetime import datetime
from konfiguration import project_mannheim


addressen_immendingen = ['st.scheible@gmail.com', 'michael.prosch@daimler.com', 'stephan.floren@daimler.com', 'bernhard.lischka@daimler.com', 'ralf.mayer@daimler.com', 'suad.sehic@daimler.com', 'benjamin.stahlmann@daimler.com', 'nathalie.wich@daimler.com', 'joshua.haag@daimler.com', 'stefan.scheible@kurz-fischer.com', 'christian.hettig@kurz-fischer.com']
addressen_development = ["stefan.scheible@kurz-fischer.com", 'st.scheible@gmail.com']

def sende_warnmail(
    io, zeitpunkt_ueberschreitung,
    beurteilungspegel,
    ):
    try:
        logging.info("Warnmail soll gesendet werden")
        # selected_time = (dt.datetime.now())
        if 5 < zeitpunkt_ueberschreitung.hour < 22:
            prozentuale_auslastung = 100*10**(0.1*beurteilungspegel) / 10**(0.1*io.Grenzwert_tag)
        else:
            prozentuale_auslastung = 100*10**(0.1*beurteilungspegel) / 10**(0.1*io.Grenzwert_nacht)
        # logging.info(f"{prozentuale_auslastung}, {beurteilungspegel}, {io.Grenzwert_nacht}")
        fromaddr = "alarm_dauerauswertung_immendingen@kurz-fischer.de"
        # toaddrs = "stefan.scheible@kurz-fischer.de, st.scheible@gmail.com, michael.prosch@daimler.com, lars.l.heinrich@daimler.com, stephan.floren@daimler.com, bernhard.lischka@daimler.com, ralf.mayer@daimler.com, suad.sehic@daimler.com, benjamin.stahlmann@daimler.com, nathalie.wich@daimler.com, joshua.haag@daimler.com, stefan.scheible@kurz-fischer.de, christian.hettig@kurz-fischer.de"
        # toaddrs = "stefan.scheible@kurz-fischer.de, st.scheible@gmail.com, michael.prosch@daimler.com, lars.l.heinrich@daimler.com, dorde.milovanovic@daimler.com, stephan.floren@daimler.com, bernhard.lischka@daimler.com, ralf.mayer@daimler.com, suad.sehic@daimler.com, benjamin.stahlmann@daimler.com, nathalie.wich@daimler.com, joshua.haag@daimler.com, stefan.scheible@kurz-fischer.de, christian.hettig@kurz-fischer.de"
        # toaddrs = "stefan.scheible@kurz-fischer.de, st.scheible@gmail.com"
        address_array = addressen_immendingen
        # address_array = addressen_development
        toaddrs = ", ".join(address_array)
        host = "syscp3.webhosting-franken.de"
        port = 25
        login = "alarm.svantek@kurz-fischer.de"
        password = "kfnum@alarm7"

        msg = EmailMessage()
        msg["Subject"] = "Lärmüberschreitungswarnung IO {0}".format(io.Id)
        msg.set_content(
            """
Am {0} wurde um {1} ein Beurteilungspegel von {2} dB an Immissionsort {3} festgestellt.
Das entspricht einer Auslastung von {4}%.
Bitte entsprechende Maßnahmen einleiten.
""".format(
            zeitpunkt_ueberschreitung.strftime("%d.%m.%y"),
            zeitpunkt_ueberschreitung.strftime("%H:%M"),
            "{:2.1f}".format(beurteilungspegel),
            str(io.Id) + ": " + io.Bezeichnung,
            "{:2.1f}".format(prozentuale_auslastung)))
        msg["From"] = fromaddr
        msg["To"] = toaddrs
        msg["Date"] = formatdate(localtime=True)

        server = smtplib.SMTP(host, port)
        server.login(login, password)
        server.send_message(msg)
        logging.info("Mail wurde gesendet")
        server.quit()
    except Exception as ex:
           logging.exception("An error occured") 

if __name__ == "__main__":
    pass # sende_warnmail(project_mannheim.IOs[0], datetime.now(), 45)



def create_beurteilungszeitraum_datetime_dict():
    return dict(zip([0,1,2,3,4,5,6,7,8], [datetime(1900, 1,1)]*9))

last_send_mail_dict = dict(zip([io.Id for io in ios], [create_beurteilungszeitraum_datetime_dict() for io in ios]))
print(last_send_mail_dict)



if __name__ == '__main__':
    FORMAT = '%(filename)s %(lineno)d %(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        level=logging.INFO, format=FORMAT, handlers=[logging.FileHandler("../logs/check_ueberschreitung.log"),
        logging.StreamHandler(sys.stdout)]
    )
    prozentualer_grenzwert = 0.9
    while True:
        try:
            for io in ios:
                zu_pruefender_zeitpunkt = datetime.now() - timedelta(minutes=15)
                if zu_pruefender_zeitpunkt.hour >= 6 and zu_pruefender_zeitpunkt.hour < 22:
                    zu_pruefender_wert = 10*log10(prozentualer_grenzwert*10**(0.1*io.Grenzwert_tag))
                else:
                    zu_pruefender_wert = 10*log10(prozentualer_grenzwert*10**(0.1*io.Grenzwert_nacht))
                id_current_beurteilungszeitraum = get_id_corresponding_beurteilungszeitraum(zu_pruefender_zeitpunkt)
                start_end_gepruefter_beurteilungszeitraum = get_interval_beurteilungszeitraum_from_datetime(zu_pruefender_zeitpunkt)
                gepruefter_beurteilungszeitraum_start = datetime(zu_pruefender_zeitpunkt.year, zu_pruefender_zeitpunkt.month, zu_pruefender_zeitpunkt.day) + timedelta(seconds=start_end_gepruefter_beurteilungszeitraum[0])
                gepruefter_beurteilungszeitraum_ende = datetime(zu_pruefender_zeitpunkt.year, zu_pruefender_zeitpunkt.month, zu_pruefender_zeitpunkt.day) + timedelta(seconds=start_end_gepruefter_beurteilungszeitraum[1])
                logging.info(f"Pruefe: {io.Id} mit {zu_pruefender_wert} in {id_current_beurteilungszeitraum}")
                try:
                    ueberschreitungsinfo = query_4_ueberschreitung_lr(io.Id, zu_pruefender_wert, gepruefter_beurteilungszeitraum_start.isoformat() + "Z", gepruefter_beurteilungszeitraum_ende.isoformat() + "Z")
                    if  ueberschreitungsinfo.pegel > 0:
                        letzte_mail_vor_hinreichend_langer_zeit = (zu_pruefender_zeitpunkt - last_send_mail_dict[io.Id][id_current_beurteilungszeitraum]).total_seconds() >= 23*3600
                        logging.info(letzte_mail_vor_hinreichend_langer_zeit)
                        if letzte_mail_vor_hinreichend_langer_zeit:
                            mail_already_sent = True
                            last_send_mail_dict[io.Id][id_current_beurteilungszeitraum] = datetime.now()
                            logging.info(f"Sending mail for {io.Id}")
                            sende_warnmail(io, ueberschreitungsinfo.erste_ueberschreitung, ueberschreitungsinfo.pegel)

                        else:
                            logging.info(f"Es wurde bereit eins Mail wegen {io.Id} versendet")
                except Exception as ex:
                    logging.exception(ex)
            logging.info("Going to sleep...")
            sleep(300)
        except KeyboardInterrupt:
            logging.info("User ended evaluation...")
            logging.info(last_send_mail_dict)
            exit()
        except Exception as ex:
            logging.exception(ex)
            raise ex

