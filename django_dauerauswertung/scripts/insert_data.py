from tsdb.models import Messpunkt, Immissionsort, Projekt, Schallursache, LrPegel, Rejected, Rejection, Detected, Detection, EvaluationMesspunkt, ExecutedEvaluation
from datetime import datetime, timedelta


def create_debug_auswertungslauf():
    selected_date = datetime(2022, 12, 1, 10, 0, 0)
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
def create_debug_project():
    p0 = Projekt()
    p0.name = "Debug"
    p0.save()
    for i in range(1, 3):
        m = Messpunkt()
        m.name = f"MP {i}"
        m.projekt = p0
        m.save()

        v = Schallursache()
        v.name = f"MP {i}"
        v.gemessen_an = m
        v.save()

        for n in ["Grille", "Vogel", "LAFeq", "Zug"]:
            r = Rejection()
            r.name = n
            r.messpunkt = m
            r.save()

    for i in ["Vorbeifahrt Immendingen MP 5", "Zugerkennung Mannheim"]:
        e = Detection()
        e.name = i
        e.save()




    for i in range(1,3):
        io = Immissionsort()
        io.name = f"IO {i}"
        io.projekt = p0
        io.save()

    


def run():
    if True:
        create_debug_project()


        if False:
            p1 = Projekt()
            p1.name = "Daimler Immendingen"
            p1.save()

            p2 = Projekt()
            p2.name = "Daimler Mannheim"
            p2.save()

            p3 = Projekt()
            p3.name = "Daimler Sindelfingen"
            p3.save()

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
            p.verursacht = Schallursache.objects.get(id=1)
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
    

    
