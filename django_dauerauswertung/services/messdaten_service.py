from dataclasses import dataclass
import math
from time import timezone
from typing import List
import pandas as pd
from datetime import datetime
import numpy as np
from constants import terzfrequenzen
from DTO import Messpunkt

import pytz

from sqlalchemy import create_engine
import logging


if True:
    @dataclass
    class MessdatenServiceV3:
        alchemyEngine = create_engine(
            'postgresql://postgres:password@127.0.0.1:5432/tsdb')
        


        
        def __post_init__(self):
            # Connect to PostgreSQL server
            self.dbConnection = self.alchemyEngine.connect()
        

        

        def get_resudaten_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
            q = f"select lafeq, lcfeq, lafmax, time from \"tsdb_resu\" where messpunkt_id = {messpunkt.id_in_db} and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME"
            print("Query", q)
            resu_df = pd.read_sql(
            q, self.dbConnection)

            
            data_dict = {
                "lafeq": f"R{messpunkt.Id}_LAFeq",
                "lafmax": f"R{messpunkt.Id}_LAFmax",
                "lcfeq": f"R{messpunkt.Id}_LCFeq",
                "time": "Timestamp"
            }
            resu_df.rename(columns=data_dict, inplace=True)
            # print(resu_df["Timestamp"].iloc[0].tzinfo)
            resu_df['Timestamp'] = resu_df['Timestamp'].dt.tz_convert('Europe/Berlin')
            # cet = pytz.timezone('CET').utcoffset()
            # resu_df['Timestamp'] = resu_df['Timestamp'] + cet
            resu_df['Timestamp'] = resu_df['Timestamp'].dt.tz_localize(None)
            resu_df.set_index("Timestamp", inplace=True)
            logging.debug(resu_df)
            return resu_df


        def get_resu_all_mps(self, ids_only, from_date, to_date):

            df_mps = []
            for i in ids_only:
                df_mps.append(self.get_resudaten_single(i, from_date, to_date))
            result = pd.concat(df_mps, axis=1, join="inner")
            return result


        def get_terz_all_mps(self, ids_only: List[Messpunkt], from_date, to_date):

            df_mps = []
            for i in ids_only:
                df_mps.append(self.get_terzdaten_single(i, from_date, to_date))
            result = pd.concat(df_mps, axis=1, join="inner")
            return result

        def get_terzdaten_single(self,messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
            try:
                terz_prefix = "LZeq"
                available_cols_terz = []
                cols_terz = []
                values_terz = []
                cols_in_db = ["time"]
                for k in terzfrequenzen:
                    available_cols_terz.append(terz_prefix + k)
                    cols_in_db.append("hz" + k)
                    
                t_arr = [f"""T{messpunkt.Id}"""]  # "T2", "T3", "T4", "T5", "T6"]
                for i in available_cols_terz:
                    for j in t_arr:
                        cols_terz.append(f"{j}_{i}")
                terz_df = pd.read_sql(
                f"select {','.join(cols_in_db)} from \"tsdb_terz\" where messpunkt_id = {messpunkt.id_in_db} and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME", self.dbConnection)



                terz_df.rename(columns={"hz20": f"T{messpunkt.Id}_LZeq20", "hz25": f"T{messpunkt.Id}_LZeq25", "hz31_5": f"T{messpunkt.Id}_LZeq31_5", "hz40": f"T{messpunkt.Id}_LZeq40", "hz50": f"T{messpunkt.Id}_LZeq50", "hz63": f"T{messpunkt.Id}_LZeq63", "hz80": f"T{messpunkt.Id}_LZeq80", "hz100": f"T{messpunkt.Id}_LZeq100", "hz125": f"T{messpunkt.Id}_LZeq125", "hz160": f"T{messpunkt.Id}_LZeq160",
                                        "hz200": f"T{messpunkt.Id}_LZeq200", "hz250": f"T{messpunkt.Id}_LZeq250", "hz315": f"T{messpunkt.Id}_LZeq315", "hz400": f"T{messpunkt.Id}_LZeq400", "hz500": f"T{messpunkt.Id}_LZeq500", "hz630": f"T{messpunkt.Id}_LZeq630", "hz800": f"T{messpunkt.Id}_LZeq800", "hz1000": f"T{messpunkt.Id}_LZeq1000", "hz1250": f"T{messpunkt.Id}_LZeq1250", "hz1600": f"T{messpunkt.Id}_LZeq1600",
                                        "hz2000": f"T{messpunkt.Id}_LZeq2000", "hz2500": f"T{messpunkt.Id}_LZeq2500", "hz3150": f"T{messpunkt.Id}_LZeq3150", "hz4000": f"T{messpunkt.Id}_LZeq4000", "hz5000": f"T{messpunkt.Id}_LZeq5000", "hz6300": f"T{messpunkt.Id}_LZeq6300", "hz8000": f"T{messpunkt.Id}_LZeq8000", "hz10000": f"T{messpunkt.Id}_LZeq10000", "hz12500": f"T{messpunkt.Id}_LZeq12500", "hz16000": f"T{messpunkt.Id}_LZeq16000",
                                        "hz20000": f"T{messpunkt.Id}_LZeq20000", "time": "Timestamp"}, inplace=True)
                
                # print(terz_df)
                terz_df['Timestamp'] = terz_df['Timestamp'].dt.tz_convert('Europe/Berlin')
                terz_df['Timestamp'] = terz_df['Timestamp'].dt.tz_localize(None)
                # cet = pytz.timezone('CET').utcoffset()
                # resu_df['Timestamp'] = resu_df['Timestamp'] + cet

                terz_df.set_index("Timestamp", inplace=True)
                return terz_df
            except Exception as e:
                logging.warning(f"MP {messpunkt.id_in_db} at {from_date.astimezone()} failed")
                logging.warning(e)
                raise e





        def get_metedaten(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
            rename_dict = {
                "time": "Timestamp",
                "winddirection": "Windrichtung",
                "rain":"Regen",
                "windspeed": "MaxWindgeschwindigkeit",
                
            }
            mete_df = pd.read_sql(f"select time, rain, temperature, windspeed, pressure, humidity, winddirection from \"tsdb_mete\" where messpunkt_id = {messpunkt.id_in_db} and time >= '{from_date.astimezone()}' and time < '{to_date.astimezone()}' ORDER BY TIME", self.dbConnection)

            mete_df.rename(columns=rename_dict, inplace=True)

            mete_df['Timestamp'] = mete_df['Timestamp'].dt.tz_convert('Europe/Berlin')
            mete_df['Timestamp'] = mete_df['Timestamp'].dt.tz_localize(None)
            mete_df.set_index("Timestamp", inplace=True)

            return mete_df


@dataclass
class RandomMessdatenService:
    def get_resudaten_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:

        total_seconds = int((to_date - from_date).total_seconds() + 1)
        dti = pd.date_range(from_date, to_date,
            freq="s", name="Timestamp")
        data_dict = {
            f"R{messpunkt.Id}_LAFeq": 15*np.random.rand(total_seconds) + 60,
            f"R{messpunkt.Id}_LAFmax": 15*np.random.rand(total_seconds) + 80,
            f"R{messpunkt.Id}_LCFeq": 15*np.random.rand(total_seconds) + 100
                        }
        df = pd.DataFrame(data_dict, index=dti)
        print(df)
        return df


    def get_resu_all_mps(self, ids_only, from_date, to_date):

        df_mps = []
        for i in ids_only:
            df_mps.append(self.get_resudaten_single(i, from_date, to_date))
        result = pd.concat(df_mps, axis=1, join="inner")
        return result


    def get_terz_all_mps(self, ids_only, from_date, to_date):
        df_mps = []
        for i in ids_only:
            df_mps.append(self.get_terzdaten_single(i, from_date, to_date))
        result = pd.concat(df_mps, axis=1, join="inner")
        return result
        # return self.get_terzdaten(ids_only, from_date, to_date)


    def get_terzdaten_single(self, messpunkt: Messpunkt, from_date: datetime, to_date: datetime) -> pd.DataFrame:
        total_seconds = int((to_date - from_date).total_seconds()) + 1
        
        terz_prefix = "LZeq"
        available_cols_terz = []
        
        cols_terz = []
        values_terz = []
        for k in terzfrequenzen:
            available_cols_terz.append(terz_prefix + k)
        t_arr = [f"""T{messpunkt.Id}"""]  # "T2", "T3", "T4", "T5", "T6"]
        for i in available_cols_terz:
            for j in t_arr:
                cols_terz.append(f"{j}_{i}")
                values_terz.append(5 * np.random.rand(total_seconds) + 50)
        dti = pd.date_range(
            from_date, to_date,
            freq="s",
            name="Timestamp")
        data_dict = dict(zip(cols_terz, values_terz))
        df = pd.DataFrame(data_dict, index=dti)
        if False:
            df.rename(columns={"hz20": f"T{mp_id}_LZeq20", "hz25": f"T{mp_id}_LZeq25", "hz31_5": f"T{mp_id}_LZeq31_5", "hz40": f"T{mp_id}_LZeq40", "hz50": f"T{mp_id}_LZeq50", "hz63": f"T{mp_id}_LZeq63", "hz80": f"T{mp_id}_LZeq80", "hz100": f"T{mp_id}_LZeq100", "hz125": f"T{mp_id}_LZeq125", "hz160": f"T{mp_id}_LZeq160",
                                    "hz200": f"T{mp_id}_LZeq200", "hz250": f"T{mp_id}_LZeq250", "hz315": f"T{mp_id}_LZeq315", "hz400": f"T{mp_id}_LZeq400", "hz500": f"T{mp_id}_LZeq500", "hz630": f"T{mp_id}_LZeq630", "hz800": f"T{mp_id}_LZeq800", "hz1000": f"T{mp_id}_LZeq1000", "hz1250": f"T{mp_id}_LZeq1250", "hz1600": f"T{mp_id}_LZeq1600",
                                    "hz2000": f"T{mp_id}_LZeq2000", "hz2500": f"T{mp_id}_LZeq2500", "hz3150": f"T{mp_id}_LZeq3150", "hz4000": f"T{mp_id}_LZeq4000", "hz5000": f"T{mp_id}_LZeq5000", "hz6300": f"T{mp_id}_LZeq6300", "hz8000": f"T{mp_id}_LZeq8000", "hz10000": f"T{mp_id}_LZeq10000", "hz12500": f"T{mp_id}_LZeq12500", "hz16000": f"T{mp_id}_LZeq16000",
                                    "hz20000": f"T{mp_id}_LZeq20000"}, inplace=True)
        return df




    def read_mete_data_v1(from_date, to_date):
        return RandomMessdatenService.get_metedaten(from_date.year, from_date.month, from_date.day, from_date.hour*3600, (to_date.hour)*3600+to_date.minute*60+to_date.second)

    def get_metedaten(self, messpunkt_id: int, from_date: datetime, to_date: datetime) -> pd.DataFrame:
        cols_mete = ["Regen", "MaxWindgeschwindigkeit", "Windrichtung"]
        total_seconds = int((to_date - from_date).total_seconds() + 1)
        values_mete = [np.floor(np.random.rand(total_seconds) + 0.001), 2 * np.random.rand(total_seconds) + 1.65,
                        np.floor(360*np.random.rand(total_seconds))]
        dti = pd.date_range(
            from_date,
            to_date,
            freq="s", name="Timestamp")
        data_dict = dict(zip(cols_mete, values_mete))
        df = pd.DataFrame(data_dict, index=dti)

        return df





if __name__ == "__main__":
    if False:
        print(RandomMessdatenService.get_resudaten(2022, 12, 1, 2*3600, 3*3600))
        print(RandomMessdatenService.get_terzdaten(2022, 12, 1, 2*3600, 3*3600))
        print(RandomMessdatenService.get_metedaten(2022, 12, 1, 2*3600, 3*3600))
    if True:
        m = MessdatenServiceV3()
        from_date = datetime(2022, 10, 2, 0, 0, 0)
        to_date = datetime(2022, 10, 2, 1, 0, 0)
        print(m.get_resudaten_single(7, from_date, to_date))
        print(m.get_resu_all_mps([1, 3, 5, 6], from_date, to_date))
        print(m.get_metedaten(2, from_date, to_date))
        # print(m.get_terzdaten_single(2, from_date, to_date))
        print(m.get_terz_all_mps([1, 3, 5, 6], from_date, to_date))