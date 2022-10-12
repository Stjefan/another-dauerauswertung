from dataclasses import dataclass
from enum import Enum
from glob import glob
import logging
from logging import config

import scripts.insert_svantek_file as read_svantek_file
import sys


FORMAT = '%(filename)s %(asctime)s %(message)s'
logging.basicConfig(
    level=logging.DEBUG, format=FORMAT, handlers=[logging.FileHandler("logs/push_files.log"),
    logging.StreamHandler(sys.stdout)]
    )


class Messfiletyp(Enum):
    version_07_21_ohne_wetterdaten = 0
    version_07_21_mit_wetterdaten = 1
    
@dataclass
class MessdatenFolder:
    folder_path: str
    name_messpunkt: str
    typ: Messfiletyp

    def get_typ_messfile(self):
        return self.typ

checked_folder = [
    MessdatenFolder(r"C:\CSV Zielordner\MB Im MP1 - Handlingkurs\Geloescht\*", "Immendingen MP 1", Messfiletyp.version_07_21_ohne_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Im MP2 - Bertha Leitstand\Geloescht\*", "Immendingen MP 2",  Messfiletyp.version_07_21_mit_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Im MP3 - Stadtstraße\Geloescht\*","Immendingen MP 3",  Messfiletyp.version_07_21_ohne_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Im MP4 - Innenstadt\Geloescht\*", "Immendingen MP 4",Messfiletyp.version_07_21_ohne_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Im MP5 - Fernstraßenoval\Geloescht\*", "Immendingen MP 5", Messfiletyp.version_07_21_ohne_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Im MP6 - Stadtstraße Heidestrecke\Geloescht\*","Immendingen MP 6",  Messfiletyp.version_07_21_ohne_wetterdaten),
]

folders_korrelationsmessung = [
    MessdatenFolder(r"C:\CSV Zielordner\MB Im IO17 - Am Hewenegg 8\Geloescht/*", "Immendingen IO 17", Messfiletyp.version_07_21_ohne_wetterdaten)
]

folders_mannheim = [
    MessdatenFolder(r"C:\CSV Zielordner\DT MA MP2\Geloescht/*", "Mannheim MP 2",  Messfiletyp.version_07_21_ohne_wetterdaten)
]

folders_sindelfingen = [
    MessdatenFolder(r"C:\CSV Zielordner\MB Sifi MP1 - Bau 34\Geloescht/*", "Sindelfingen MP 1",  Messfiletyp.version_07_21_ohne_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Sifi MP2 - Bau 46\Geloescht/*", "Sindelfingen MP 2",  Messfiletyp.version_07_21_mit_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Sifi MP3 - Bau 7_4 Penthouse\Geloescht/*", "Sindelfingen MP 3",  Messfiletyp.version_07_21_ohne_wetterdaten),
    # MessdatenFolder(r"C:\CSV Zielordner\MB Sifi MP4 - Bau 50_12\Geloescht/*", "Sindelfingen MP 4",  Messfiletyp.version_07_21_ohne_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Sifi MP4 - Bau 50_12\Geloescht/Geloescht 2021\Unzipped/*", "Sindelfingen MP 4",  Messfiletyp.version_07_21_ohne_wetterdaten),
    MessdatenFolder(r"C:\CSV Zielordner\MB Sifi MP5 - Bau 17_4\Geloescht/*", "Sindelfingen MP 5",  Messfiletyp.version_07_21_ohne_wetterdaten)
]

def get_files(folder, target_date_as_string):
    print(f"args: {folder}, {target_date_as_string}")
    list_of_files = glob(folder)

    print(len(list_of_files))

    filtered_files = [fil for fil in list_of_files if target_date_as_string in fil] 
    return filtered_files

def read_and_push(file_name, messpunkt_name, messpunkt_typ):
    if messpunkt_typ == Messfiletyp.version_07_21_mit_wetterdaten:
        df_resu, df_terz, df_mete = read_svantek_file.process_svantek_rtm(file_name)
        read_svantek_file.push_2_api(df_resu, df_terz, df_mete, file_name, messpunkt_name)
    else:
        df_resu, df_terz = read_svantek_file.process_svantek_rt(file_name)
        read_svantek_file.push_2_api(df_resu, df_terz, None, file_name, messpunkt_name)

def insert_files_from_folder(project_name: str, folder: MessdatenFolder, filter_string: str = "202209"):
    logging.info(folder)
    for fil in get_files(folder.folder_path, filter_string):
        logging.info(fil)

        if False: #"20220601" in fil or "20220602" in fil or "20220603" in fil or "20220604" in fil:
            logging.info(f"I continue with next element: {fil}")
            continue
        else:
            try:
                df_resu = None
                df_terz = None
                df_mete = None
                if folder.typ == Messfiletyp.version_07_21_mit_wetterdaten:
                    df_resu, df_terz, df_mete = read_svantek_file.process_svantek_rtm(fil)
                else:
                    df_resu, df_terz = read_svantek_file.process_svantek_rt(fil)

            except Exception as e:
                logging.exception(e)
                # raise e


def process_data_file(file_path: str, project_name: str, messpunkt_name: str, messfile_typ: Messfiletyp):
    try:
        df_resu = None
        df_terz = None
        df_mete = None
        if messfile_typ == Messfiletyp.version_07_21_mit_wetterdaten:
            df_resu, df_terz, df_mete = read_svantek_file.process_svantek_rtm(file_path)
        elif messfile_typ ==  Messfiletyp.version_07_21_ohne_wetterdaten:
            df_resu, df_terz = read_svantek_file.process_svantek_rt(file_path)

    except Exception as e:
        logging.exception(e)
        # raise e

if __name__ == '__main__':
    selected_month = "202209"
    if False:
        for folder in [f for f in checked_folder]:
            insert_files_from_folder("immendingen", folder)
    elif False:
        for folder in folders_korrelationsmessung:
            insert_files_from_folder("immendingen_korrelationsmessung", folder)
    elif False:
        for i in range(2, 5):
            selected_month = f"2021{i:02}"
            for folder in [f for f in folders_sindelfingen if f.name_messpunkt in ["Sindelfingen MP 4"]]:
                insert_files_from_folder("sindelfingen", folder, selected_month)
        
    else:
        for m in ["202209"]:
            for folder in folders_mannheim:
                insert_files_from_folder("mannheim", folder, m)