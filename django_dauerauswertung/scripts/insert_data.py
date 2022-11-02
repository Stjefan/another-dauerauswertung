
from tsdb.models import (Messpunkt, Immissionsort, Projekt,
        LaermursacheAnMesspunkt, LaermursacheAnImmissionsorten,
        LrPegel, Rejected, Rejection, Detected, Detection, EvaluationMesspunkt, Auswertungslauf, Ausbreitstungsfaktor
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

def update_mps():
    ["C:\CSV Zielordner\MB Im MP1 - Handlingkurs\Geloescht\*"]
    ["C:\CSV Zielordner\DT MA MP2\Geloescht/*"]

def create_immendingen_project():
    p0 = Projekt()
    p0.name = "Immendingen"
    p0.save()

    m1 = Messpunkt(name="Immendingen MP 1", gk_rechts = 3479801.64, gk_hoch = 5308413.74, projekt = p0, id_external = 1)
    m1.save()
    v1a = LaermursacheAnMesspunkt(name =  f"{m1.name} - Unkategorisiert", gemessen_an = m1)
    v1a.save()
    m2 = Messpunkt(name="Immendingen MP 2", gk_rechts = 3478651.2, gk_hoch = 5308912.9, projekt = p0, id_external = 2)
    m2.is_meteo_station = True
    m2.save()
    v2a = LaermursacheAnMesspunkt(name =  f"{m2.name} - Unkategorisiert", gemessen_an = m2)
    v2a.save()
    
    m3 = Messpunkt(name="Immendingen MP 3", gk_rechts = 3479665.2, gk_hoch = 5310121.2, projekt = p0, id_external = 3)
    m3.save()
    v1a = LaermursacheAnMesspunkt(name =  f"{m3.name} - Unkategorisiert", gemessen_an = m3)
    v1a.save()
    m4 = Messpunkt(name="Immendingen MP 4", gk_rechts = 3480498.61, gk_hoch = 5309049.1, projekt = p0, id_external = 4)
    m4.save()
    v1a = LaermursacheAnMesspunkt(name =  f"{m4.name} - Unkategorisiert", gemessen_an = m4)
    v1a.save()
    m5 = Messpunkt(name="Immendingen MP 5", gk_rechts = 3479604.83, gk_hoch = 5309170.3, projekt = p0, id_external = 5)
    m5.save()
    v5a = LaermursacheAnMesspunkt(name =  f"{m5.name} - Unkategorisiert", gemessen_an = m5)
    v5a.save()
    v5b = LaermursacheAnMesspunkt(name =  f"{m5.name} - Vorbeifahrt", gemessen_an = m5)
    v5b.save()
    m6 = Messpunkt(name="Immendingen MP 6", gk_rechts = 3480633.4, gk_hoch = 5310332.6, projekt = p0, id_external = 6)
    m6.save()
    v6a = LaermursacheAnMesspunkt(name =  f"{m6.name} - Unkategorisiert", gemessen_an = m6)
    v6a.save()

    # mps_mit_ereignissen = [
    #         Messpunkt(Id=1, bezeichnung_in_db="Immendingen MP 1",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3479801.64, 5308413.74), Bezeichnung="MP 1"),
    #         Messpunkt(Id=2, bezeichnung_in_db="Immendingen MP 2",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3478651.2, 5308912.9), Bezeichnung="MP 2"),
    #         Messpunkt(Id=3, bezeichnung_in_db="Immendingen MP 3",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3479665.2, 5310121.2), Bezeichnung="MP 3"),
    #         Messpunkt(Id=4, bezeichnung_in_db="Immendingen MP 4",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3480498.61, 5309049.1), Bezeichnung="MP 4"),
    #         Messpunkt(Id=5,bezeichnung_in_db="Immendingen MP 5", Ereignisse=["ohne_ereignis", "vorbeifahrt"], Koordinaten=Koordinaten(3479604.83, 5309170.3), Bezeichnung="MP 5"),
    #         Messpunkt(Id=6,bezeichnung_in_db="Immendingen MP 6", Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3480633.4, 5310332.6), Bezeichnung="MP 6"),

    #     ]

    io1 = Immissionsort(name = "Immendingen - Bachzimmererstr. 32", gk_rechts = 3480042.7, gk_hoch = 5311610.6, id_external = 1, name_4_excel = "Bachzimmererstr. 32", grenzwert_tag = 36, grenzwert_nacht = 30, projekt = p0)
    io1.save()
    io5 = Immissionsort(name = "Immendingen - Ziegelhütte 4", gk_rechts = 3480369.2, gk_hoch = 5310724.4, id_external = 5, name_4_excel = "Ziegelhütte 4", grenzwert_tag = 40, grenzwert_nacht = 37,  projekt = p0)
    io5.save()
    io9 = Immissionsort(name = "Zimmern - Kreutzerweg. 4", gk_rechts = 3478899.7, gk_hoch = 5310874.2, id_external = 9, name_4_excel = "Kreutzerweg. 4", grenzwert_tag = 38, grenzwert_nacht = 32, projekt = p0)
    io9.save()
    io15 = Immissionsort(name = "Am Hewenegg 1", gk_rechts = 3480671.6, gk_hoch = 5308875.2, id_external = 15, name_4_excel = "Am Hewenegg 1", grenzwert_tag = 46, grenzwert_nacht = 42, projekt = p0)
    io15.save()
    io17 = Immissionsort(name = "Am Hewenegg 8", gk_rechts = 3480435.3, gk_hoch =5308574.5, id_external = 17, name_4_excel = "Am Hewenegg 8", grenzwert_tag = 52, grenzwert_nacht = 42,  projekt = p0)
    io17.save()

    # ios_immendingen = [
    # Immissionsort(Bezeichnung = "Immendingen - Bachzimmererstr. 32", Id = 1, Grenzwert_tag = 36, Grenzwert_nacht = 30, Koordinaten = Koordinaten(GKRechtswert = 3480042.7, GKHochwert = 5311610.6), shortname_for_excel="Bachzimmererstr. 32"),
    # Immissionsort(Bezeichnung = "Immendingen - Ziegelhütte 4", Id = 5, Grenzwert_tag = 40, Grenzwert_nacht = 37, Koordinaten = Koordinaten(GKRechtswert = 3480369.2, GKHochwert = 5310724.4), shortname_for_excel="Ziegelhütte 4"), # Hier stand Urpsrunglich 4
    # Immissionsort(Bezeichnung = "Zimmern - Kreutzerweg. 4", Id = 9, Grenzwert_tag = 38, Grenzwert_nacht = 32, Koordinaten = Koordinaten(GKRechtswert = 3478899.7, GKHochwert = 5310874.2), shortname_for_excel="Kreutzerweg. 4"),
    # Immissionsort(Bezeichnung = "Am Hewenegg 1", Id = 15, Grenzwert_tag = 46, Grenzwert_nacht = 42, Koordinaten = Koordinaten(GKRechtswert = 3480671.6, GKHochwert = 5308875.2), shortname_for_excel="Am Hewenegg 1"),
    # Immissionsort(Bezeichnung = "Am Hewenegg 8", Id = 17, Grenzwert_tag = 52, Grenzwert_nacht = 42, Koordinaten = Koordinaten(GKRechtswert = 3480435.3, GKHochwert = 5308574.5), shortname_for_excel="Am Hewenegg 8"),
    # ]

    a1_1 = Ausbreitstungsfaktor(immissionsort=io1, messpunkt=m1, ausbreitungskorrektur = -46.3)
    a1_2 = Ausbreitstungsfaktor(immissionsort=io1, messpunkt=m2, ausbreitungskorrektur = -43.1)
    a1_3 = Ausbreitstungsfaktor(immissionsort=io1, messpunkt=m3, ausbreitungskorrektur = -33.3)
    a1_4 = Ausbreitstungsfaktor(immissionsort=io1, messpunkt=m4, ausbreitungskorrektur = -31.2)
    a1_5 = Ausbreitstungsfaktor(immissionsort=io1, messpunkt=m5, ausbreitungskorrektur = -35.1)
    a1_6 = Ausbreitstungsfaktor(immissionsort=io1, messpunkt=m6, ausbreitungskorrektur = -32.3)

    arr_a1 = [a1_1, a1_2, a1_3, a1_4, a1_5, a1_6]

    a5_1 = Ausbreitstungsfaktor(immissionsort=io5, messpunkt=m1, ausbreitungskorrektur = -51.2)
    a5_2 = Ausbreitstungsfaktor(immissionsort=io5, messpunkt=m2, ausbreitungskorrektur = -41.6)
    a5_3 = Ausbreitstungsfaktor(immissionsort=io5, messpunkt=m3, ausbreitungskorrektur = -31)
    a5_4 = Ausbreitstungsfaktor(immissionsort=io5, messpunkt=m4, ausbreitungskorrektur = -36.6)
    a5_5 = Ausbreitstungsfaktor(immissionsort=io5, messpunkt=m5, ausbreitungskorrektur = -35.4)
    a5_6 = Ausbreitstungsfaktor(immissionsort=io5, messpunkt=m6, ausbreitungskorrektur = -34.6)

    arr_a5 = [a5_1, a5_2, a5_3, a5_4, a5_5, a5_6]

    a9_1 = Ausbreitstungsfaktor(immissionsort=io9, messpunkt=m1, ausbreitungskorrektur = -43.3)
    a9_2 = Ausbreitstungsfaktor(immissionsort=io9, messpunkt=m2, ausbreitungskorrektur = -38.7)
    a9_3 = Ausbreitstungsfaktor(immissionsort=io9, messpunkt=m3, ausbreitungskorrektur = -29.9)
    a9_4 = Ausbreitstungsfaktor(immissionsort=io9, messpunkt=m4, ausbreitungskorrektur = -31.5)
    a9_5 = Ausbreitstungsfaktor(immissionsort=io9, messpunkt=m5, ausbreitungskorrektur = -32.7)
    a9_6 = Ausbreitstungsfaktor(immissionsort=io9, messpunkt=m6, ausbreitungskorrektur = -32.9)

    arr_a9 = [a9_1, a9_2, a9_3, a9_4, a9_5, a9_6]

    a15_1 = Ausbreitstungsfaktor(immissionsort=io15, messpunkt=m1, ausbreitungskorrektur = -31.8)
    a15_2 = Ausbreitstungsfaktor(immissionsort=io15, messpunkt=m2, ausbreitungskorrektur = -33.7)
    a15_3 = Ausbreitstungsfaktor(immissionsort=io15, messpunkt=m3, ausbreitungskorrektur = -24.7)
    a15_4 = Ausbreitstungsfaktor(immissionsort=io15, messpunkt=m4, ausbreitungskorrektur = -15.9)
    a15_5 = Ausbreitstungsfaktor(immissionsort=io15, messpunkt=m5, ausbreitungskorrektur = -27.8)
    a15_6 = Ausbreitstungsfaktor(immissionsort=io15, messpunkt=m6, ausbreitungskorrektur = -30.7)

    arr_a15 = [a15_1, a15_2, a15_3, a15_4, a15_5, a15_6]

    a17_1 = Ausbreitstungsfaktor(immissionsort=io17, messpunkt=m1, ausbreitungskorrektur = -25.6)
    a17_2 = Ausbreitstungsfaktor(immissionsort=io17, messpunkt=m2, ausbreitungskorrektur = -30.3)
    a17_3 = Ausbreitstungsfaktor(immissionsort=io17, messpunkt=m3, ausbreitungskorrektur = -31.1)
    a17_4 = Ausbreitstungsfaktor(immissionsort=io17, messpunkt=m4, ausbreitungskorrektur = -17.2)
    a17_5 = Ausbreitstungsfaktor(immissionsort=io17, messpunkt=m5, ausbreitungskorrektur = -21.7)
    a17_6 = Ausbreitstungsfaktor(immissionsort=io17, messpunkt=m6, ausbreitungskorrektur = -30.2)

    arr_a17 = [a17_1, a17_2, a17_3, a17_4, a17_5, a17_6]

    for a in arr_a1 + arr_a5 + arr_a9 + arr_a15 + arr_a17:
        a.save()


    for mp in p0.messpunkt_set.all():
        for u in mp.laermursacheanmesspunkt_set.all():
            ursache_an_io = LaermursacheAnImmissionsorten(name=u.name, projekt=p0)
            ursache_an_io.save()
    ursache_an_io = LaermursacheAnImmissionsorten(name="Gesamt", projekt=p0)
    ursache_an_io.save()

    dict_abf = {
            (1, 1): -46.3, (5, 1): -51.2, (9, 1): -43.3, (15, 1): -31.8, (17, 1): -25.6,
            (1, 2): -43.1, (5, 2): -41.6, (9, 2): -38.7, (15, 2): -33.7, (17, 2): -30.3,
            (1, 3): -33.3, (5, 3): -31.0, (9, 3): -29.9, (15, 3): -24.7, (17, 3): -31.1,
            (1, 4): -31.2, (5, 4): -36.6, (9, 4): -31.5, (15, 4): -15.9, (17, 4): -17.2,
            (1, 5): -35.1, (5, 5): -35.4, (9, 5): -32.7, (15, 5): -27.8, (17, 5): -21.7, #-20.8, -14.7
            (1, 6): -32.3, (5, 6): -34.6, (9, 6): -32.9, (15, 6): -30.7, (17, 6): -30.2
        }

def create_mannheim_project():
    p0 = Projekt(name="Mannheim")
    p0.save()


    m2 = Messpunkt(name="Mannheim MP 2", gk_rechts =  49.52145, gk_hoch=8.48382, projekt = p0, id_external = 2)
    m2.save()
    v2a = LaermursacheAnMesspunkt(name =  f"{m2.name} - Unkategorisiert", gemessen_an = m2)
    v2a.save()

    io4 = Immissionsort(name="Fichtenweg 2", grenzwert_tag=55,
                  grenzwert_nacht=45, gk_rechts=49.5232, gk_hoch=8.4872, id_external = 4, projekt=p0 )
    io4.save()

    io5 = Immissionsort(name="Speckweg 18", grenzwert_tag=55,
                  grenzwert_nacht=45, gk_rechts=49.52333, gk_hoch=8.48428, id_external = 5, projekt=p0  )
    io5.save()
    io6 =  Immissionsort(name="Spiegelfabrik 16", grenzwert_tag=55,
                  grenzwert_nacht=45, gk_rechts=49.5191, gk_hoch=8.47877, id_external = 6, projekt=p0 )
    io6.save()
    
    dict_abf_mannheim = {
    (4, 1): -15.7, (5, 1): -21.6, (6, 1): -17
    }
    a4_2 = Ausbreitstungsfaktor(immissionsort=io4, messpunkt=m2, ausbreitungskorrektur = -15.7)
    a5_2 = Ausbreitstungsfaktor(immissionsort=io5, messpunkt=m2, ausbreitungskorrektur = -21.6)
    a6_2 = Ausbreitstungsfaktor(immissionsort=io6, messpunkt=m2, ausbreitungskorrektur = -17)

    arr_2 = [a4_2, a5_2, a6_2]
    for a in arr_2:
        a.save()

    for mp in p0.messpunkt_set.all():
        for u in mp.laermursacheanmesspunkt_set.all():
            ursache_an_io = LaermursacheAnImmissionsorten(name=u.name, projekt=p0)
            ursache_an_io.save()
    ursache_an_io = LaermursacheAnImmissionsorten(name="Gesamt", projekt=p0)
    ursache_an_io.save()

    


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

    
    

        
def create_sindelfingen_project():
    pass


    

def insert_utm():
    
    


    ios = [
    {
      "id": 4,
      "name": "Fichtenweg 2",
      "position": [49.523202, 8.4872387],
    },
    {
      "id": 5,
      "name": "Speckweg 18",
      "position": [49.523326, 8.48195],
    },
    {
      "id": 6,
      "name": "Spiegelfabrik 16",
      "position": [49.519069, 8.47877],
    }
    ]
    
    mps = [
        {
        "id": 2,
        "name": "Mannheim MP 2",
        "position": [49.521449, 8.4838235],
        "name_in_api": "Mannheim MP 2",
        }
    ]
    for i in ios:
        io = Immissionsort.objects.get(name__icontains=i["name"])
        io.utm_x = i["position"][0]
        io.utm_y = i["position"][1]
        io.save()
    for i in mps:
        io = Messpunkt.objects.get(name__icontains=i["name"])
        io.utm_x = i["position"][0]
        io.utm_y = i["position"][1]
        io.save()
    mps = [
        {
        "id": 1,
        "name": "MP 1 - Handlingkurs",
        "name_in_api": "Immendingen MP 1",
        "position": [47.912783, 8.728536],
        },
        {
        "id": 2,
        "name": "MP 2 - Bertha Leitstand",
        "name_in_api": "Immendingen MP 2",
        "position": [47.91799, 8.71139],
        },
        {
        "id": 3,
        "name": "MP 3 - Stadtstraße",
        "name_in_api": "Immendingen MP 3",
        "position": [47.928551, 8.725036],
        },
        {
        "id": 4,
        "name": "MP 4 - Innenstadt",
        "name_in_api": "Immendingen MP 4",
        "position": [47.919428, 8.738175],
        },
        {
        "id": 5,
        "name": "MP 5 - Fernstraßenoval",
        "name_in_api": "Immendingen MP 5",
        "position": [47.920379146443693, 8.72609453922426],
        },
        {
        "id": 6,
        "name_in_api": "Immendingen MP 6",
        "name": "MP 6 - Stadtraße Heidestrecke",
        "position": [47.930765, 8.739706],
        },
    ]

    ios = [
        {
        "name": "Bachzimmererstr. 32",
        "id": 1,
        "gw_tag": 36,
        "gw_nacht": 30,
        "position": [47.930765, 8.731831],
        },
        {
        "name": "Ziegelhütte 4",
        "id": 5,
        "gw_tag": 40,
        "gw_nacht": 37,
        "position": [47.934338, 8.736257],
        },
        {
        "name": "Kreutzerweg",
        "id": 9,
        "gw_tag": 38,
        "gw_nacht": 32,
        "position": [47.93568, 8.716572],
        },
        {
        "name": "Am Hewenegg 1",
        "id": 15,
        "gw_tag": 46,
        "gw_nacht": 42,
        "position": [47.917715, 8.740678],
        },
        {
        "name": "Am Hewenegg 8",
        "id": 17,
        "gw_tag": 52,
        "gw_nacht": 42,
        "position": [47.915059, 8.737231],
        }
    ]
    for i in ios:
        print(i["name"])
        io = Immissionsort.objects.get(name__icontains=i["name"])
        io.utm_x = i["position"][0]
        io.utm_y = i["position"][1]
        io.save()
    for i in mps:
        io = Messpunkt.objects.get(name__icontains=i["name_in_api"])
        io.utm_x = i["position"][0]
        io.utm_y = i["position"][1]
        io.save()



def run():
    if True:
        insert_utm()
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
        for p in Projekt.objects.all():
                p.delete()

        create_immendingen_project()
        create_mannheim_project()
    if False:
        for p in Projekt.objects.all():
            p.delete()
        # create_debug_project()

    if False:
        m =Messpunkt.objects.get(name="Immendingen MP 2")
        m.is_meteo_station = True
        m.save()


        


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
    

    
