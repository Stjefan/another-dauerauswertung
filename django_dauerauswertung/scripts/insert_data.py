from tsdb.models import (Messpunkt, Immissionsort, Projekt,
        LaermursacheAnMesspunkt, LaermursacheAnImmissionsorten,
        LrPegel, Rejected, Rejection, Detected, Detection, EvaluationMesspunkt, Auswertungslauf
)
from datetime import datetime, timedelta


def create_debug_auswertungslauf():
    selected_date = datetime(2022, 12, 1, 10, 0, 0)
    e = Auswertungslauf()
    if True:
        for i in range(0, 20):
            n = Detected()
            n.typ = Detection.objects.get(id=1)
            n.time = selected_date + timedelta(minutes=i)
            n.dauer = 12
            n.save()

    if True:
        for i in range(0, 20):
            for ii in range(0, 25):
                n = Rejected()
                n.filter = Rejection.objects.get(id=1)
                n.time = selected_date + timedelta(minutes=i, seconds=ii+25)
                n.save()

def create_immendingen_project():
    p0 = Projekt()
    p0.name = "Immendingen"
    p0.save()

def create_mannheim_project():
    p0 = Projekt()
    p0.name = "Mannheim"
    p0.save()


def create_debug_project():
    p0 = Projekt()
    p0.name = "Debug Immendingen"
    p0.save()
    for i in range(1, 6):
        m = Messpunkt()
        m.name = f"{p0.name} - MP {i}"
        m.projekt = p0
        m.save() 

        if i != 4:
            v = LaermursacheAnMesspunkt()
            v.name = f"{p0.name} - MP {i}"
            v.gemessen_an = m
            v.save()
        else:
            for l in [f"MP {i}", f"Vorbeifahrt {i}"]:
                v = LaermursacheAnMesspunkt()
                v.name = f"{l}"
                v.gemessen_an = m
                v.save()


    for i in range(1,5):
        io = Immissionsort()
        io.name = f"{p0.name} - IO {i}"
        io.projekt = p0
        io.save()

    for mp in p0.messpunkt_set.all():
        for u in mp.laermursacheanmesspunkt_set.all():
            ursache_an_io = LaermursacheAnImmissionsorten()
            ursache_an_io.name = u.name
            ursache_an_io.projekt = p0
            ursache_an_io.save()
    ursache_an_io = LaermursacheAnImmissionsorten()
    ursache_an_io.projekt = p0
    ursache_an_io.name = "Gesamt"
    ursache_an_io.save()

    for u in LaermursacheAnImmissionsorten.objects.filter(projekt_id=p0.id):
        print(u.name)        

    
    

        



    


def run():
    if True:
        for p in Projekt.objects.all():
            p.delete()
        create_debug_project()


        if False:
            for n in ["Grille", "Vogel", "LAFeq", "Zug", "Wind", "Regen"]:
                r = Rejection()
                r.name = n
                r.save()

            for i in ["Vorbeifahrt Immendingen MP 5", "Zugerkennung Mannheim"]:
                e = Detection()
                e.name = i
                e.save()


    if False:
        for i in range(2, 6):
            m = Messpunkt()
            m.name = f"MP {i}"
            m.save()
    if False:
        for i in range(1, 6):
            io = Immissionsort()
            io.name = f"IO {i}"
            io.save()  
    if False:
        for i in range(1, 6):
            v = Rejection()
            v.name = f"Lafeq - MP {i}"
            v.messpunkt = Messpunkt.objects.get(id=1)
            v.save()
    if False:
        for i in range(0, 12):
            p = LrPegel()
            p.immissionsort = Immissionsort.objects.get(id=1)
            p.pegel = 42
            p.verursacht = LaermursacheAnMesspunkt.objects.get(id=1)
            p.time = datetime(2022, 10, 6, 11, 0, 0) + timedelta(minutes=5*i)
            p.save()
    

    if False:
        for i in range(10*60, 11*60):
            current_length = randrange(0, 60)
            print(current_length)
            for ii in range(0, current_length):                
                n = Detected()
                n.typ = Detection.objects.get(id=1)
                n.time = datetime(2022, 8, 1, 0, 0, 0) + timedelta(minutes=i, seconds=ii)
                n.dauer = 12
                n.save()
    

    
