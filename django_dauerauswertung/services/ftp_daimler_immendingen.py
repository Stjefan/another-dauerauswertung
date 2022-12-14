
from calendar import monthrange
import ftplib
from io import BytesIO
import logging
import socket
import ssl

from ftplib import FTP, FTP_TLS
from datetime import datetime, timedelta
from time import sleep

import requests
from auswertung_service import get_project_via_rest
from fun_with_blockfiles import get_lrpegel_csv, create_block_csv_mete, create_block_csv_resu, create_block_csv_terz
import ftplib, socket, ssl
FTPTLS_OBJ = ftplib.FTP_TLS


# stolen from: https://stackoverflow.com/questions/12164470/python-ftp-implicit-tls-connection-issue

# Class to manage implicit FTP over TLS connections, with passive transfer mode
# - Important note:
#   If you connect to a VSFTPD server, check that the vsftpd.conf file contains
#   the property require_ssl_reuse=NO
class FTPTLS(FTPTLS_OBJ):

    host = "127.0.0.1"
    port = 990
    user = "anonymous"
    timeout = 60

    logLevel = 0

    # Init both this and super
    def __init__(self, host=None, user=None, passwd=None, acct=None, keyfile=None, certfile=None, context=None, timeout=60):        
        FTPTLS_OBJ.__init__(self, host, user, passwd, acct, keyfile, certfile, context, timeout)

    # Custom function: Open a new FTPS session (both connection & login)
    def openSession(self, host="127.0.0.1", port=990, user="anonymous", password=None, timeout=60):
        self.user = user
        # connect()
        ret = self.connect(host, port, timeout)
        # prot_p(): Set up secure data connection.
        try:
            ret = self.prot_p()
            if (self.logLevel > 1): self._log("INFO - FTPS prot_p() done: " + ret)
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS prot_p() failed - " + str(e))
            raise e
        # login()
        try:
            ret = self.login(user=user, passwd=password)
            if (self.logLevel > 1): self._log("INFO - FTPS login() done: " + ret)
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS login() failed - " + str(e))
            raise e
        if (self.logLevel > 1): self._log("INFO - FTPS session successfully opened")

    # Override function
    def connect(self, host="127.0.0.1", port=990, timeout=60):
        self.host = host
        self.port = port
        self.timeout = timeout
        try:
            self.sock = socket.create_connection((self.host, self.port), self.timeout)
            self.af = self.sock.family
            self.sock = ssl.wrap_socket(self.sock, self.keyfile, self.certfile)
            self.file = self.sock.makefile('r')
            self.welcome = self.getresp()
            if (self.logLevel > 1): self._log("INFO - FTPS connect() done: " + self.welcome)
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS connect() failed - " + str(e))
            raise e
        return self.welcome

    # Override function
    def makepasv(self):
        host, port = FTPTLS_OBJ.makepasv(self)
        # Change the host back to the original IP that was used for the connection
        host = socket.gethostbyname(self.host)
        return host, port

    # Custom function: Close the session
    def closeSession(self):
        try:
            self.close()
            if (self.logLevel > 1): self._log("INFO - FTPS close() done")
        except Exception as e:
            if (self.logLevel > 0): self._log("ERROR - FTPS close() failed - " + str(e))
            raise e
        if (self.logLevel > 1): self._log("INFO - FTPS session successfully closed")

    # Private method for logs
    def _log(self, msg):
        # Be free here on how to implement your own way to redirect logs (e.g: to a console, to a file, etc.)
        print(msg)


host = "test.rebex.net"
port = 990
user = "demo"
password = "password"

host = "ftp.daimler-tss.de"
user = "ftp_pia"
password = "fmz2TmCLQ339mZ76"
port = 990


    # print(ftp_tls_daimler.dir(f'./PIA/KURZ_FISCHER/Fixed_ids/{messpunkt_name}/{current_hour.strftime("%Y-%m")}/{current_hour.strftime("%d")}/{current_hour.strftime("%H")}'))




def get_file():
    return open("test.txt", "rb")


def push_beurteilungspegel_2_ftp_server(ftp_tls_daimler, end_date: datetime):
    try:
        logging.info("Starting...")
        to_date = datetime(end_date.year, end_date.month, end_date.day) + timedelta(days=1) # + timedelta(hours=4) #
        from_date = to_date + timedelta(days=-7) # + timedelta(hours=-3) #
        filename = f"""ios_immendingen_rolling_{from_date.strftime("%Y_%m_%d")}_To_{end_date.strftime("%Y_%m_%d")}.csv"""
        fullpath_on_ftp_server = f"""./PIA/KURZ_FISCHER/Immissionspegel/{filename}"""
        path_for_dev = f"""C:/Users/sts/Documents/Github/another-dauerauswertung/dev/{filename}"""
        with get_lrpegel_csv(from_date, to_date) as f:
            if False:
                with open(path_for_dev, "wb") as fil:
                    fil.write(f.getbuffer())
            else:

                ftp_tls_daimler.storbinary(f'STOR {fullpath_on_ftp_server}', f)
                logging.info("Storing completed")
                if False:
                    with open(path_for_dev, "wb") as fil:
                        fil.write(f.getbuffer())
        logging.info("Ending...")
    except Exception as e:
        logging.exception(e)
    

# push_beurteilungspegel_2_ftp_server()



def make_dir_recursive(path):
    path_arr = path.split("/")



def push_messdaten_2_ftp_server(ftp_tls_daimler, current_hour, mp, messpunkt_name = "Innendstadt", messpunkt_seriennummer = "57044", debug=False):
    target_dir = "C:/Users/sts/Documents/Github/another-dauerauswertung/dev/"
    try:
        
        try:
            ftp_tls_daimler.mkd(
                f"""./PIA/KURZ_FISCHER/Fixed_ids/{messpunkt_name}/{current_hour.strftime("%Y-%m")}""")
        except ftplib.error_perm as e:
            logging.info(e)
            logging.info("Continue...")
        try:
            ftp_tls_daimler.mkd(
                f"""./PIA/KURZ_FISCHER/Fixed_ids/{messpunkt_name}/{current_hour.strftime("%Y-%m")}/{current_hour.strftime("%d")}""")
        except ftplib.error_perm as e:
            logging.info(e)
            logging.info("Continue...")
        for i in range(0, 23):

            try:
                selected_time = current_hour + timedelta(hours=i)
                
                ftp_tls_daimler.mkd(
                    f"""./PIA/KURZ_FISCHER/Fixed_ids/{messpunkt_name}/{current_hour.strftime("%Y-%m")}/{current_hour.strftime("%d")}/{selected_time.strftime("%H")}""")
            except ftplib.error_perm as e:
                logging.info(e)
                logging.info("Continue...")
            try:
                fun_array = [(create_block_csv_resu, "RESU"), (create_block_csv_terz, "TERZ")]
                if mp.Id == 2:
                    fun_array += [(create_block_csv_mete, "METE")]
                for fun, name in fun_array:
                    with fun(mp, selected_time, selected_time + timedelta(hours=1)) as f:
                        full_filename_on_server = f"""./PIA/KURZ_FISCHER/Fixed_ids/{messpunkt_name}/{current_hour.strftime("%Y-%m")}/{current_hour.strftime("%d")}/{selected_time.strftime("%H")}/{messpunkt_seriennummer}_{name}_{selected_time.strftime("%Y_%m_%d_%H")}.csv"""
                        # print(full_filename_on_server)
                        if debug:
                            path_for_dev = f"""C:/Users/sts/Documents/Github/another-dauerauswertung/dev/{messpunkt_name}_{name}_{selected_time.strftime("%Y_%m_%d_%H")}.csv"""
                            # ftp_tls_daimler.storbinary(f'STOR {full_filename_on_server}', f)
                            with open(path_for_dev, "wb") as fil:
                                fil.write(f.getbuffer())
                        else:
                            ftp_tls_daimler.storbinary(f'STOR {full_filename_on_server}', f)
            except Exception as e:
                logging.exception(e)
    except Exception as e:
        logging.exception(e)
        raise e    
    

some_date = datetime.now()

messpunkte_seriennummer = ["57046", "57045", "57072", "57059", "57044", "57062"]
messpunkte_bezeichnung = ['Fernstraßenoval', 'Innenstadt', 'Heideareal', 'Stadtstraße', 'Handlingkurs', 'Bertha-Leitstand']

combined = zip(messpunkte_bezeichnung, messpunkte_seriennummer)
zuordnung_ids_immendingen = [5, 4,6,3,1,2]# ["MP 5", "MP 4", "MP 6", "MP 3", "MP 1", "MP 2"]


my_dict = dict(zip(zuordnung_ids_immendingen, combined))

p = get_project_via_rest("immendingen")

myFtps = FTPTLS()
myFtps.logLevel = 2
myFtps.openSession(host, port, user, password)



logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        
        # logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ])
# push_beurteilungspegel_2_ftp_server(day)

single_day = True

if single_day:
    
    selected_day = datetime(2022, 9, 1)

    if True:
        push_beurteilungspegel_2_ftp_server(myFtps, selected_day)
    if True:
        for mp in p.MPs:
            push_messdaten_2_ftp_server(myFtps, selected_day, mp, my_dict[mp.Id][0], my_dict[mp.Id][1])

else:
    day = datetime(2022, 9, 1)
    days_in_month = monthrange(day.year, day.month)[1]
    for d in range(0, min(100, days_in_month)):
        selected_day = day + timedelta(days=d)

        if True:
            push_beurteilungspegel_2_ftp_server(myFtps, selected_day)
        if True:
            for mp in p.MPs:
                push_messdaten_2_ftp_server(myFtps, selected_day, mp, my_dict[mp.Id][0], my_dict[mp.Id][1])


myFtps.closeSession()