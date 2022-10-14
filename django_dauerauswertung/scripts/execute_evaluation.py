from random import Random
from tsdb.models import (Projekt, Messpunkt, Immissionsort, Projekt,
 LaermursacheAnMesspunkt, LrPegel, Rejected, Rejection, Detected, Detection, EvaluationMesspunkt, Auswertungslauf, LaermursacheAnImmissionsorten)
from datetime import datetime, timedelta
from django.db import transaction





def get_beurteilungszeitraum_start(arg: datetime):
    if 6<= arg.hour <= 21:
        return datetime(arg.year, arg.month, arg.day, 6, 0, 0), datetime(arg.year, arg.month, arg.day, 21, 59, 59)
    else:
        return datetime(arg.year, arg.month, arg.day, arg.hour, 0, 0), datetime(arg.year, arg.month, arg.day, arg.hour, 0, 0) + timedelta(hours=1, seconds=-1)

def get_hours_in_beurteilungszeitraum(arg: datetime):
    if 6<= arg.hour <= 21:
        return 16
    else: return 1


def create_debug_auswertungslauf():
    with transaction.atomic():
        p = Projekt.objects.get(name="Debug Immendingen")
        ursachen = p.laermursacheanimmissionsorten_set.all()

        
        
        selected_date, end_selected_beurteilungszeitraum = get_beurteilungszeitraum_start(datetime(2022, 12, 2, 1, 0, 0))
        former_runs = Auswertungslauf.objects.filter(zeitpunkt_im_beurteilungszeitraum__gte=selected_date, zeitpunkt_im_beurteilungszeitraum__lte=end_selected_beurteilungszeitraum, zuordnung=p)
        hours_in_beurteilungszeitraum = get_hours_in_beurteilungszeitraum(selected_date)
        print(len(former_runs))
        for r in former_runs:
            r.delete()
        rejections =  Rejection.objects.filter()
        ios = Immissionsort.objects.filter(projekt = p)
        print(ursachen)
        print(rejections)

        e = Auswertungslauf()
        e.in_berechnung_gewertete_messwerte = 3600
        e.zeitpunkt_durchfuehrung = datetime.now()
        e.zuordnung = p
        e.verhandene_messwerte = 900
        e.verwertebare_messwerte = 600
        e.zeitpunkt_im_beurteilungszeitraum = selected_date
        e.save()
        if True:
            for i in range(0, 20):
                n = Detected()
                n.typ = Detection.objects.get(id=1)
                n.time = selected_date + timedelta(minutes=i)
                n.dauer = 12
                n.berechnet_von = e
                n.save()

        if True:
            for idx, rej in enumerate(rejections):
                for i in range(5*(idx), 5*(idx+1)):
                    for ii in range(0, 25):
                        n = Rejected()
                        n.filter = rej
                        n.time = selected_date + timedelta(minutes=i, seconds=ii+25)
                        n.berechnet_von = e
                        n.save()
        if True:
            r = Random()
            for io in ios:
                for u in ursachen:
                    m = r.choice(seq=[0, 1, 2, 3, 4])
                    for h in range(0, hours_in_beurteilungszeitraum):
                        for i in range(0, 12):
                            tp = selected_date + timedelta(hours=h, minutes=5*i, seconds=-1)
                            p = LrPegel()
                            p.immissionsort = io
                            
                            p.verursacht = u
                            p.time = tp
                            p.berechnet_von = e
                            if "Gesamt" in u.name:
                                m = 5
                                p.pegel = m*(h*12+i)+5
                            else:
                                p.pegel = m*(h*12+i)+4
                            p.save()
                    
                        

def run():
    # create_debug_auswertungslauf()
    create_debug_auswertungslauf()