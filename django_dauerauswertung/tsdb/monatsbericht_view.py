from calendar import monthrange
import locale
import typing
import xlsxwriter

from django.http import HttpResponse, HttpResponseNotFound

from io import BytesIO
from django.db import connection
from datetime import datetime, timedelta
from django.conf import settings
import os



from tsdb.models import Projekt



from tsdb.helpers import Monatsbericht, MonatsuebersichtAnImmissionsortV2
from tsdb.monatsbericht_creation import erstelle_xslx_monatsbericht


kufi_fußzeile_thumbnail = "C:/Users/sts/Documents/GitHub/another-dauerauswertung/django_dauerauswertung/static/ressources/kufi_fußzeile_thumbnail.jpg"
kufi_logo_thumbnail = "C:/Users/sts/Documents/GitHub/another-dauerauswertung/django_dauerauswertung/static/ressources/kf_logo_thumbnail.jpg"

def foo(request):
    
    projekt_name = request.GET.get("projekt", "Immendingen")
    month = int(request.GET.get("month", datetime.now().month))
    year = int(request.GET.get("year", datetime.now().year))



    with connection.cursor() as cursor:

        m = read_data_monatsbericht(cursor, month, year, projekt_name)

        output = BytesIO()

        erstelle_xslx_monatsbericht(m, output)

        if False:

            workbook = xlsxwriter.Workbook(output, {'remove_timezone': True})
            
            worksheet = workbook.add_worksheet("Blub")

            print(os.getcwd(), settings.STATIC_URL, os.path.join(settings.STATIC_URL, kufi_logo_thumbnail))

            worksheet.set_header('&C&G&R\n\n\nSeite &P von &N', {"image_center": kufi_logo_thumbnail, 'margin': 0.5})
            worksheet.set_footer('&L&F&C&G&RDatum: &D', {"image_center": kufi_fußzeile_thumbnail, 'margin': 0.6 / 2.54})

            worksheet.write('A1', 'Hello')

        # Some data we want to write to the worksheet.
        if False:
            monatsauswertung, general_information, lr_tagzeitraum, max_pegel_tagzeitraum, lr_nachtzeitraum = read_data_monatsbericht(cursor)
            row = 0
            for line in monatsauswertung:
                worksheet.write(row, 5, line[0])
                worksheet.write(row, 6, line[1])
                worksheet.write(row, 7, line[2])
                row += 1

            row = 5
            for line in lr_tagzeitraum:
                worksheet.write(row, 10, line[0])
                worksheet.write(row, 11, line[1])
                row += 1
            row = 5
            for line in max_pegel_tagzeitraum:
                worksheet.write(row, 14, line[0])
                worksheet.write(row, 15, line[1])
                row += 1

            row = 5
            for line in lr_nachtzeitraum:
                worksheet.write(row, 18, line[1])
                worksheet.write(row, 19, line[2])
                row += 1

            worksheet.write(1, 10, "Verfuegbar")
            worksheet.write(1, 11, general_information[2])

            worksheet.write(1, 14, "Wetter")
            worksheet.write(1, 15, general_information[3]) 

            worksheet.write(1, 18, "Sonstiges")
            worksheet.write(1, 19, general_information[4]) 


        

        try:
            if True:
                # sending response 
                response = HttpResponse(output.getvalue(), content_type='application/vnd.ms-excel')
                response['Content-Disposition'] = f'attachment; filename=Monatsbericht_{m.projekt.name}_{year}_{month}_alpha.xlsx'
            # response = HttpResponse('<h2>Hello</h2>')

        except IOError:
            # handle file not exist case here
            response = HttpResponseNotFound('<h1>File not exist</h1>')

        print(settings.STATIC_URL)

        return response





def read_data_monatsbericht(cursor, month: int, year:int, projekt_name: str):
    days_in_month = monthrange(year, month)[1]
    current_time = datetime(year, month, 1)

    p = Projekt.objects.get(name__iexact=projekt_name)

    projekt_id = p.id
    q_tz = """SET TIME ZONE 'Europe/Rome'"""
    after_time =   datetime(current_time.year, current_time.month, 1, 0, 0, 0)
    before_time = after_time+ timedelta(days=days_in_month)
    

    q_filtered = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id = {projekt_id}"""


    q_wetter_filter = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id = {projekt_id} AND (rej.filter_id = 4 or rej.filter_id = 5)"""
    q_sonstige_filter = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id ={projekt_id} AND rej.filter_id != 4 and rej.filter_id != 5"""

    q_schalllesitungspegel = f"""SELECT count(*) FROM tsdb_rejected rej JOIN tsdb_messpunkt m ON rej.messpunkt_id = m.id WHERE time >= '{after_time}' AND time <= '{before_time}' AND m.projekt_id = 1"""

    q_verfuegbare_sekunden = f"""SELECT sum(verwertebare_messwerte) FROM tsdb_auswertungslauf WHERE zeitpunkt_im_beurteilungszeitraum >= '{after_time}' AND zeitpunkt_im_beurteilungszeitraum <= '{before_time}' AND zuordnung_id = {projekt_id};"""

    q_schallleistungpegel = f"""SELECT * FROM tsdb_schallleistungpegel WHERE time >= '{after_time}' AND time <= '{before_time}' AND messpunkt_id = 4;"""

    


    cursor.execute(q_tz)
    # print(q_verfuegbare_sekunden)
    cursor.execute(q_verfuegbare_sekunden)
    verfuegbare_sekunden = cursor.fetchall()[0][0]
    print(verfuegbare_sekunden)

    print(q_filtered)
    cursor.execute(q_filtered)
    

    aussortierte_sekunden = cursor.fetchall()[0][0]
    print(aussortierte_sekunden)

    # print(q_sonstige_filter)
    cursor.execute(q_sonstige_filter)
    aussortierte_sekunden_sonstiges = cursor.fetchall()[0][0]
    print(aussortierte_sekunden_sonstiges)


    # print(q_wetter_filter)
    cursor.execute(q_wetter_filter)
    aussortierte_sekunden_wetter = cursor.fetchall()[0][0]
    print(aussortierte_sekunden_wetter)



    

    details_io = {}
    m = Monatsbericht(after_time, p, verfuegbare_sekunden, aussortierte_sekunden_wetter, aussortierte_sekunden_sonstiges, "Hello", details_io)

    for io in p.immissionsort_set.all():
        immissionsort_id = io.id

        if io.name_4_excel == "":
            io.name_4_excel = io.name
            io.save()

        q_max_pegel_night = f"""
        SELECT extract('day' from time), pegel, extract('hour' from time) AS Stunde FROM (
            SELECT time::date AS Date, time, pegel, ROW_NUMBER() OVER (
                PARTITION BY time::date
                ORDER BY pegel DESC, time
            ) as rank FROM tsdb_maxpegel 
            WHERE time >= '{after_time}' AND time <= '{before_time}' AND (time::time <= '06:00' OR time::time >= '22:00') AND immissionsort_id = {immissionsort_id}) T1
        WHERE T1.rank = 1 ORDER BY Date;
        """

        q_max_pegel_day = f"""
            SELECT extract('day' from time), pegel, extract('hour' from time) AS Stunde FROM (
                SELECT time::date AS Date, time, pegel, ROW_NUMBER() OVER (
                PARTITION BY time::date
	            ORDER BY pegel DESC, time
            ) as rank FROM tsdb_maxpegel
            WHERE time >= '{after_time}' AND time <= '{before_time}' AND (time::time >= '06:00' OR time::time <= '22:00') AND immissionsort_id = {immissionsort_id}) T1
            WHERE T1.rank = 1;
        """

        q_day = f"""
    SELECT extract('day' from time::date), max(pegel) FROM tsdb_lrpegel lr WHERE time >= '{after_time}' AND time <= '{before_time}' AND immissionsort_id = {immissionsort_id} GROUP BY time::date;
    """

        q_night = f"""
        SELECT  extract('day' from T1.time), T1.pegel, argMax FROM (
            SELECT time::date AS time, date_part( 'hour', time) AS argMax, max(pegel) AS pegel FROM tsdb_lrpegel lr WHERE time >= '{after_time}' AND time <= '{before_time}' AND time::time <= '06:00' AND immissionsort_id = {immissionsort_id} GROUP BY time::date, date_part('hour', time)) T1
        JOIN tsdb_lrpegel T2 On T1.time = T2.time::date and T1.pegel = T2.pegel AND T2.immissionsort_id = {immissionsort_id};
        """

        # print(q_max_pegel_night)
        cursor.execute(q_max_pegel_night)
        max_pegel_mit_stunde = cursor.fetchall()
        result_maxpegel_nacht = {}
        for row in max_pegel_mit_stunde:
            result_maxpegel_nacht[row[0]] = (row[1], int(row[2]))


        cursor.execute(q_day)
        lr_tagzeitraum = cursor.fetchall()
        result_lr_tagzeitraum = {}
        for row in lr_tagzeitraum:
            result_lr_tagzeitraum[row[0]] = row[1]


        cursor.execute(q_night)
        lr_nachtzeitraum = cursor.fetchall()
        result_lr_nachtzeitraum = {}
        for row in lr_nachtzeitraum:
            result_lr_nachtzeitraum[row[0]] = (row[1], int(row[2]))

        
        print(q_max_pegel_day)
        cursor.execute(q_max_pegel_day)
        max_pegel_tagzeitraum = cursor.fetchall()
        result_maxpegel_tag = {}
        for row in max_pegel_tagzeitraum:
            result_maxpegel_tag[row[0]] = row[1]

        details = MonatsuebersichtAnImmissionsortV2(io, result_lr_tagzeitraum, result_lr_nachtzeitraum, result_maxpegel_tag, result_maxpegel_nacht)
        details_io[io.id] = details

    return m


