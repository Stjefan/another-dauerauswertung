from DTO import Immissionsort, Koordinaten, Messpunkt, Projekt


mps_sindelfingen = [
    Messpunkt(Id=1, bezeichnung_in_db="Sindelfingen MP 1", Bezeichnung="Bau 34", Ereignisse=["ohne_ereignis"], LWA=48.2, Koordinaten=Koordinaten(3478651.2, 5308912.9), OrdnerMessdaten="C:\CSV Zielordner\MB Sifi MP1 - Bau 34"),
    Messpunkt(Id=5, bezeichnung_in_db="Sindelfingen MP 5", Bezeichnung="Bau 17_4", Ereignisse=["ohne_ereignis"], LWA=50, Koordinaten=Koordinaten(3479665.2, 5310121.2), OrdnerMessdaten="C:\CSV Zielordner\MB Sifi MP5 - Bau 17_4"),
    Messpunkt(Id=3, bezeichnung_in_db="Sindelfingen MP 3", Bezeichnung="Bau 7_4", Ereignisse=["ohne_ereignis"], LWA=54.7, Koordinaten=Koordinaten(3480498.61, 5309049.1), OrdnerMessdaten="C:\CSV Zielordner\MB Sifi MP3 - Bau 7_4 Penthouse"),
    Messpunkt(Id=4, bezeichnung_in_db="Sindelfingen MP 4", Bezeichnung="Bau 50", Ereignisse=["ohne_ereignis"], LWA=50, Koordinaten=Koordinaten(3479604.83, 5309170.3), OrdnerMessdaten="C:\CSV Zielordner\MB Sifi MP4 - Bau 50_1"),
    Messpunkt(Id=2, bezeichnung_in_db="Sindelfingen MP 2", Bezeichnung="Bau 46", Ereignisse=["ohne_ereignis"], LWA=54.8, Koordinaten=Koordinaten(3480633.4, 5310332.6), OrdnerMessdaten="C:\CSV Zielordner\MB Sifi MP2 - Bau 46"),
]

ios_sindelfingen = [
    Immissionsort(Id=1, Bezeichnung="Paul-Zweigart-Straße", Grenzwert_tag=55,
                  Grenzwert_nacht=40, ruhezeitzuschlag=True, Koordinaten=Koordinaten(3499720.9, 5396710.5)),
    Immissionsort(Id=2, Bezeichnung="Altinger Straße", Grenzwert_tag=60, Grenzwert_nacht=45, Koordinaten=Koordinaten(3500178.8, 5396262.7)),
    Immissionsort(Id=3, Bezeichnung="Goldbergstraße", Grenzwert_tag=55, Grenzwert_nacht=40, ruhezeitzuschlag=True, Koordinaten=Koordinaten(3500665.3,5395670.7)),
    Immissionsort(Id=4, Bezeichnung="Aussiedlerhof", Grenzwert_tag=60, Grenzwert_nacht=45, Koordinaten=Koordinaten(3498511.4, 5396279)),
    Immissionsort(Id=5, Bezeichnung="Dagersheim", Grenzwert_tag=55, Grenzwert_nacht=40, ruhezeitzuschlag=True, Koordinaten=Koordinaten(3497516.4,5394939.34)), # Hier fehlt die Hochkoordinate # Dummy eingesetzt

]

dict_abf_sindelfingen = {
            (5,4):-36.6,(4,4):-28.9,(2,4):-14.5,(3,4):-21.5,(1,4):-23.8,
            (5,3):-34.1,(4,3):-12.3,(2,3):-30.7,(3,3):-24.1,(1,3):-14.5,
            (5,1):-37.6,(4,1):-18.9,(2,1):-43,(3,1):-33.8,(1,1):-31.9,
            (5,5):-34.3,(4,5):-29.1,(2,5):-26.9,(3,5):-22,(1,5):-30.7,
            (5,2):-21,(4,2):-21,(2,2):-40.1,(3,2):-33.9,(1,2):-35.1,
            # (6,1):-27.5,(6,5):-27.7,(6,3):-5.6,(6,4):-18.3,(6,2):-31.4,
}



mps_mit_ereignissen = [
            Messpunkt(Id=1, bezeichnung_in_db="Immendingen MP 1",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3479801.64, 5308413.74), Bezeichnung="MP 1"),
            Messpunkt(Id=2, bezeichnung_in_db="Immendingen MP 2",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3478651.2, 5308912.9), Bezeichnung="MP 2"),
            Messpunkt(Id=3, bezeichnung_in_db="Immendingen MP 3",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3479665.2, 5310121.2), Bezeichnung="MP 3"),
            Messpunkt(Id=4, bezeichnung_in_db="Immendingen MP 4",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3480498.61, 5309049.1), Bezeichnung="MP 4"),
            Messpunkt(Id=5,bezeichnung_in_db="Immendingen MP 5", Ereignisse=["ohne_ereignis", "vorbeifahrt"], Koordinaten=Koordinaten(3479604.83, 5309170.3), Bezeichnung="MP 5"),
            Messpunkt(Id=6,bezeichnung_in_db="Immendingen MP 6", Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(3480633.4, 5310332.6), Bezeichnung="MP 6"),

        ]

mps_mannheim_mit_ereignissen: list[Messpunkt] = [
    Messpunkt(Id=1, bezeichnung_in_db="Mannheim MP 2",Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(49.52145, 8.48382), Filter=["Zug"])
]

ios_mannheim: list[Immissionsort] = [
    Immissionsort(Id=4, Bezeichnung="Fichtenweg 2", Grenzwert_tag=55,
                  Grenzwert_nacht=45, Koordinaten=Koordinaten(49.5232, 8.4872)),
    Immissionsort(Id=5, Bezeichnung="Speckweg 18", Grenzwert_tag=55, Grenzwert_nacht=45, Koordinaten=Koordinaten(49.52333, 8.48428)),
    Immissionsort(Id=6, Bezeichnung="Spiegelfabrik 16", Grenzwert_tag=55, Grenzwert_nacht=45, Koordinaten=Koordinaten(49.5191, 8.47877)),
]

mps_testdaten = [{"Id": 1, "Koordinaten": Koordinaten(10, 20), "Ereignisse": ["ohne_ereignis"]},
                   {"Id": 2, "Koordinaten": Koordinaten(10, 20), "Ereignisse": ["ohne_ereignis"]},
                   {"Id": 3, "Koordinaten": Koordinaten(10, 20), "Ereignisse": ["ohne_ereignis"]},
                   {"Id": 4, "Koordinaten": Koordinaten(10, 20), "Ereignisse": ["ohne_ereignis"]}]

'''
ios_immendingen = [
    Immissionsort(Id=1, Koordinaten=Koordinaten(3480042.7, 5311610.6), Bezeichnung="Bachzimmererstr. 32"),
    Immissionsort(Id=5, Koordinaten=Koordinaten(3480369.2, 5310724.4)),
    Immissionsort(Id=9, Koordinaten=Koordinaten(3478899.7, 5310874.2)),
    Immissionsort(Id=15, Koordinaten=Koordinaten(3480671.6, 5308875.2)),
    Immissionsort(Id=17, Koordinaten=Koordinaten(3480435.3, 5308574.5))
]
'''

ios_immendingen = [
    Immissionsort(Bezeichnung = "Immendingen - Bachzimmererstr. 32", Id = 1, Grenzwert_tag = 36, Grenzwert_nacht = 30, Koordinaten = Koordinaten(GKRechtswert = 3480042.7, GKHochwert = 5311610.6), shortname_for_excel="Bachzimmererstr. 32"),
    Immissionsort(Bezeichnung = "Immendingen - Ziegelhütte 4", Id = 5, Grenzwert_tag = 40, Grenzwert_nacht = 37, Koordinaten = Koordinaten(GKRechtswert = 3480369.2, GKHochwert = 5310724.4), shortname_for_excel="Ziegelhütte 4"), # Hier stand Urpsrunglich 4
    Immissionsort(Bezeichnung = "Zimmern - Kreutzerweg. 4", Id = 9, Grenzwert_tag = 38, Grenzwert_nacht = 32, Koordinaten = Koordinaten(GKRechtswert = 3478899.7, GKHochwert = 5310874.2), shortname_for_excel="Kreutzerweg. 4"),
    Immissionsort(Bezeichnung = "Am Hewenegg 1", Id = 15, Grenzwert_tag = 46, Grenzwert_nacht = 42, Koordinaten = Koordinaten(GKRechtswert = 3480671.6, GKHochwert = 5308875.2), shortname_for_excel="Am Hewenegg 1"),
    Immissionsort(Bezeichnung = "Am Hewenegg 8", Id = 17, Grenzwert_tag = 52, Grenzwert_nacht = 42, Koordinaten = Koordinaten(GKRechtswert = 3480435.3, GKHochwert = 5308574.5), shortname_for_excel="Am Hewenegg 8"),
]
dict_abf = {
            (1, 1): -46.3, (5, 1): -51.2, (9, 1): -43.3, (15, 1): -31.8, (17, 1): -25.6,
            (1, 2): -43.1, (5, 2): -41.6, (9, 2): -38.7, (15, 2): -33.7, (17, 2): -30.3,
            (1, 3): -33.3, (5, 3): -31.0, (9, 3): -29.9, (15, 3): -24.7, (17, 3): -31.1,
            (1, 4): -31.2, (5, 4): -36.6, (9, 4): -31.5, (15, 4): -15.9, (17, 4): -17.2,
            (1, 5): -35.1, (5, 5): -35.4, (9, 5): -32.7, (15, 5): -27.8, (17, 5): -21.7, #-20.8, -14.7
            (1, 6): -32.3, (5, 6): -34.6, (9, 6): -32.9, (15, 6): -30.7, (17, 6): -30.2
        }


dict_abf_mannheim = {
    (4, 1): -15.7, (5, 1): -21.6, (6, 1): -17
}

dict_abf_immendingen_korrelation = {
    (170, 9): 0
}

ios_immendingen_korrelation = [
    Immissionsort(Bezeichnung = "Immendingen - Am Hewenegg 8", Id = 170, Grenzwert_tag = 52, Grenzwert_nacht = 42, Koordinaten = Koordinaten(GKRechtswert = 3480435.3, GKHochwert = 5308574.5)),
]

mps_immendingen_korrelation = [
    Messpunkt(Id=9, bezeichnung_in_db="Immendingen IO 17", Ereignisse=["ohne_ereignis"], Koordinaten=Koordinaten(GKRechtswert = 3480435.3, GKHochwert = 5308574.5), Bezeichnung="Immendingen IO 17")
    ]  

project_sindelfingen = Projekt("Sindelfingen", ios_sindelfingen, mps_sindelfingen, dict_abf_sindelfingen, "sindelfingen", has_mete_data=True, gw_lafeq=70, gw_lafmax=90, has_terz_data=False)

project_mannheim = Projekt("Mercedes-Benz Werk Mannheim", ios_mannheim, mps_mannheim_mit_ereignissen, dict_abf_mannheim, "mannheim")

project_immendingen = Projekt("Immendingen Dauerauswertung", ios_immendingen, mps_mit_ereignissen, dict_abf, "immendingen", has_mete_data=True)

project_debug = Projekt(0, ios_immendingen, mps_mit_ereignissen, dict_abf, "debug")

projekt_korrelationsmessung = Projekt("Korrelation Immendingen", ios_immendingen_korrelation, mps_immendingen_korrelation, dict_abf_immendingen_korrelation, "immendingen_korrelationsmessung")