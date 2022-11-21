

import argparse
from io import BytesIO
import pathlib
import json
import os
import datetime as dt
from calendar import monthrange
import locale
import typing
import xlsxwriter

import logging

from tsdb.helpers import Monatsbericht, MonatsuebersichtAnImmissionsortV2


kufi_fußzeile_thumbnail = "ressources/kufi_fußzeile_thumbnail.jpg"
kufi_logo_thumbnail = "ressources/kf_logo_thumbnail.jpg"
kufi_fußzeile_thumbnail = "C:/Users/sts/Documents/GitHub/another-dauerauswertung/django_dauerauswertung/static/ressources/kufi_fußzeile_thumbnail.jpg"
kufi_logo_thumbnail = "C:/Users/sts/Documents/GitHub/another-dauerauswertung/django_dauerauswertung/static/ressources/kf_logo_thumbnail.jpg"

gray = "#808080"
dark_gray = "#A9A9A9"
silver = "#C0C0C0"
light_gray = "#D3D3D3"
white_smoke = "#F5F5F5"
green = "#008000"


def erstelle_xslx_monatsbericht(data: Monatsbericht, output: BytesIO(), schallleistungspegel_sheet = False):
    number_days_in_month = monthrange(data.monat.year, data.monat.month)[1]

    workbook = xlsxwriter.Workbook(output)
    my_sheetname = "Monat"
    worksheet = workbook.add_worksheet(my_sheetname)


    worksheet.set_header('&C&G&R\n\n\nSeite &P von &N', {"image_center": os.path.join(os.path.dirname(__file__), kufi_logo_thumbnail), 'margin': 0.5})
    worksheet.set_footer('&L&F&C&G&RDatum: &D', {"image_center": os.path.join(os.path.dirname(__file__), kufi_fußzeile_thumbnail), 'margin': 0.6 / 2.54})

    create_content_worksheet_uebersicht(workbook, worksheet, data, number_days_in_month, data.monat)

    worksheet.set_margins(left=1.5/2.54, right=1/2.54, top=4/2.54, bottom=2.3/2.54)

    if schallleistungspegel_sheet:
        erstelle_sheet_schallleistungspegel(workbook,  data.monat.year, data.monat.month, data.schallleistungspegel)

    for key in data.details_io.keys():
        logging.info(f"Writing data for {key}")
        monatsuebersicht_an_io = data.details_io[key]
        io_details_worksheet = workbook.add_worksheet(monatsuebersicht_an_io.immissionsort.name_4_excel)
        io_details_worksheet.set_margins(left=1.5/2.54, right=1/2.54, top=4/2.54, bottom=2.3/2.54)
        io_details_worksheet.set_header('&C&G&R\n\n\nSeite &P von &N', {"image_center": os.path.join(os.path.dirname(__file__), kufi_logo_thumbnail), 'margin': 0.5})
        # io_details_worksheet.set_footer('&L&F&C&G&RDatum: &D', {"image_center": os.path.join(os.path.dirname(__file__), kufi_fußzeile_thumbnail), 'margin': 0.6 / 2.54})
        io_details_worksheet.set_footer('&C&G&R&6Datum: &D', {"image_center": os.path.join(os.path.dirname(__file__), kufi_fußzeile_thumbnail), 'margin': 0.6 / 2.54})
        create_content_worksheet_io_details(workbook, io_details_worksheet, monatsuebersicht_an_io, data.monat, number_days_in_month)
            
    

    workbook.close()
    logging.debug("Finished successfully")
    

def create_content_worksheet_io_details(wb, worksheet, io_detail_daten: MonatsuebersichtAnImmissionsortV2, first_of_month, number_days_in_month):
    curr_excel_ws = ExcelWorksheet(40, 40, wb, worksheet)
    bezeichnung_worksheet = worksheet.get_name()
    ausgewerteter_monat = first_of_month.strftime("%B %Y")
    # number_days_in_month = 30
    curr_excel_ws.make_box_around_cells(0, 3, 2, 8, 2, bg_color=white_smoke)
    curr_excel_ws.make_box_around_cells(3, 5, number_days_in_month+2, 8, 2, bg_color=silver)
    curr_excel_ws.make_box_around_cells(0, 1, 2, 2)
    curr_excel_ws.make_box_around_cells(3, 1, number_days_in_month+2, 2)

    worksheet.set_column(0, 0, 1)
    worksheet.set_column(1, 10, 9)
    worksheet.set_column(3, 3, 13)
    worksheet.set_column(4, 4, 11)
    worksheet.set_column(5, 5, 13)
    worksheet.set_column(6, 6, 9)
    worksheet.set_column(7, 7, 12)
    worksheet.set_column(8, 8, 9)

    for i in range(1, 35):
        worksheet.set_row(i, 12)
    worksheet.set_row(2, 30)

    curr_excel_ws.make_box_around_cells(3, 3, number_days_in_month + 2, 3, boxtype=1)
    curr_excel_ws.make_box_around_cells(3, 4, number_days_in_month + 2, 4, boxtype=1)
    curr_excel_ws.make_box_around_cells(3, 5, number_days_in_month + 2, 5, boxtype=1)
    curr_excel_ws.make_box_around_cells(3, 6, number_days_in_month + 2, 6, boxtype=1, bg_color=silver)
    curr_excel_ws.make_box_around_cells(3, 7, number_days_in_month + 2, 7, boxtype=1, bg_color=silver)
    curr_excel_ws.make_box_around_cells(3, 8, number_days_in_month + 2, 8, boxtype=1, bg_color=silver)

    curr_excel_ws.make_box_around_cells(3, 11, number_days_in_month+2, 11)
    curr_excel_ws.make_box_around_cells(3, 12, number_days_in_month+2, 13, bg_color=silver)
    curr_excel_ws.make_box_around_cells(0, 12, 2, 13, bg_color=white_smoke)

    curr_excel_ws.merge_cells(0, 12, 1, 13)
    curr_excel_ws.ws_cells[(0, 12)].content = "Immissions-\nrichtwerte"
    curr_excel_ws.ws_cells[(0, 12)].format.set_bold(True)
    curr_excel_ws.ws_cells[(0, 12)].format.set_align("center")

    curr_excel_ws.ws_cells[(2, 12)].content = "tags"
    curr_excel_ws.ws_cells[(2, 12)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 12)].format.set_right(1)
    curr_excel_ws.ws_cells[(2, 13)].content = "nachts"
    curr_excel_ws.ws_cells[(2, 13)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 13)].format.set_left(1)

    curr_excel_ws.merge_cells(0, 3, 0, 8)
    curr_excel_ws.ws_cells[(0, 3)].content = "Beurteilungspegel nach TA Lärm"
    curr_excel_ws.ws_cells[(0, 3)].format.set_bold(True)
    curr_excel_ws.ws_cells[(0, 3)].format.set_align("center")

    curr_excel_ws.merge_cells(1, 5, 1, 8)
    curr_excel_ws.ws_cells[(1, 5)].content = "Zeitbereich Nacht 22 - 6h"
    curr_excel_ws.ws_cells[(1, 5)].format.set_bold(True)
    curr_excel_ws.ws_cells[(1, 5)].format.set_text_wrap(True)

    curr_excel_ws.merge_cells(1, 3, 1, 4)
    curr_excel_ws.ws_cells[(1, 3)].content = "Zeitbereich Tag 6 - 22h"
    curr_excel_ws.ws_cells[(1, 3)].format.set_bold(True)
    curr_excel_ws.ws_cells[(1, 3)].format.set_text_wrap(True)


    curr_excel_ws.ws_cells[(2, 3)].content = "Beurteilungspegel [dB(A)]"
    curr_excel_ws.ws_cells[(2, 3)].format.set_text_wrap(True)
    curr_excel_ws.ws_cells[(2, 3)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 3)].format.set_right(1)
    curr_excel_ws.ws_cells[(2, 3)].format.set_font_size(8)
    curr_excel_ws.ws_cells[(2, 4)].content = "Spitzenpegel [dB(A)]"
    curr_excel_ws.ws_cells[(2, 4)].format.set_text_wrap(True)
    curr_excel_ws.ws_cells[(2, 4)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 4)].format.set_left(1)
    curr_excel_ws.ws_cells[(2, 4)].format.set_font_size(8)

    curr_excel_ws.ws_cells[(2, 5)].content = "Beurteilungspegel [dB(A)]"
    curr_excel_ws.ws_cells[(2, 5)].format.set_text_wrap(True)
    curr_excel_ws.ws_cells[(2, 5)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 5)].format.set_right(1)
    curr_excel_ws.ws_cells[(2, 5)].format.set_font_size(8)

    curr_excel_ws.ws_cells[(2, 6)].content = "Stunde"
    curr_excel_ws.ws_cells[(2, 6)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 6)].format.set_left(1)
    curr_excel_ws.ws_cells[(2, 6)].format.set_right(1)
    curr_excel_ws.ws_cells[(2, 6)].format.set_font_size(8)
    curr_excel_ws.ws_cells[(2, 7)].content = "Spitzenpegel [dB(A)]"
    curr_excel_ws.ws_cells[(2, 7)].format.set_text_wrap(True)
    curr_excel_ws.ws_cells[(2, 7)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 7)].format.set_right(1)
    curr_excel_ws.ws_cells[(2, 7)].format.set_font_size(8)

    curr_excel_ws.ws_cells[(2, 8)].content = "Stunde"
    curr_excel_ws.ws_cells[(2, 8)].format.set_align("center")
    curr_excel_ws.ws_cells[(2, 8)].format.set_left(1)
    curr_excel_ws.ws_cells[(2, 8)].format.set_font_size(8)

    for i in range(0, number_days_in_month):
        try:
            curr_excel_ws.ws_cells[(i+3, 1)].content = (first_of_month + dt.timedelta(days=i)).strftime("%A")
            curr_excel_ws.ws_cells[(i + 3, 1)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 2)].content = (first_of_month + dt.timedelta(days=i)).strftime("%d.%m.%Y")
            curr_excel_ws.ws_cells[(i + 3, 2)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 2)].format.set_align("center")

            curr_excel_ws.ws_cells[(i + 3, 12)].content = io_detail_daten.immissionsort.grenzwert_tag
            curr_excel_ws.ws_cells[(i + 3, 12)].format.set_right(1)
            curr_excel_ws.ws_cells[(i + 3, 12)].format.set_align("center")
            curr_excel_ws.ws_cells[(i + 3, 12)].format.set_font_size(8)

            curr_excel_ws.ws_cells[(i + 3, 13)].content = io_detail_daten.immissionsort.grenzwert_nacht
            curr_excel_ws.ws_cells[(i + 3, 13)].format.set_left(1)
            curr_excel_ws.ws_cells[(i + 3, 13)].format.set_align("center")
            curr_excel_ws.ws_cells[(i + 3, 13)].format.set_font_size(8)

            print(io_detail_daten.lr_max_nacht[i+1])

            curr_excel_ws.ws_cells[(i + 3, 3)].content = io_detail_daten.lr_tag[i+1]
            curr_excel_ws.ws_cells[(i + 3, 3)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(i + 3, 3)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 3)].format.set_align("center")
            curr_excel_ws.ws_cells[(i + 3, 4)].content = io_detail_daten.lauteste_stunde_tag[i+1]
            curr_excel_ws.ws_cells[(i + 3, 4)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(i + 3, 4)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 4)].format.set_align("center")
            curr_excel_ws.ws_cells[(i + 3, 5)].content = io_detail_daten.lr_max_nacht[i+1][0]
            curr_excel_ws.ws_cells[(i + 3, 5)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(i + 3, 5)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 5)].format.set_align("center")

            beginn_lauteste_stunde_lr = (first_of_month + dt.timedelta(days=i, hours=io_detail_daten.lr_max_nacht[i+1][1]))
            ende_lauteste_stunde_lr = beginn_lauteste_stunde_lr + dt.timedelta(hours=1)
            curr_excel_ws.ws_cells[(i + 3, 6)].content = f'{beginn_lauteste_stunde_lr.strftime("%H:00")} - {ende_lauteste_stunde_lr.strftime("%H:00")}'
            curr_excel_ws.ws_cells[(i + 3, 6)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 6)].format.set_align("center")

            curr_excel_ws.ws_cells[(i + 3, 7)].content = io_detail_daten.lauteste_stunde_nacht[i+1][0]
            curr_excel_ws.ws_cells[(i + 3, 7)].format.set_align("center")
            beginn_lauteste_stunde = (first_of_month + dt.timedelta(days=i, hours=io_detail_daten.lauteste_stunde_nacht[i+1][1]))
            curr_excel_ws.ws_cells[(i + 3, 7)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(i + 3, 7)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 7)].format.set_align("center")
            # Hier war ein Fehler
            ende_lauteste_stunde = beginn_lauteste_stunde + dt.timedelta(hours=1)
            curr_excel_ws.ws_cells[
                (i + 3, 8)].content = f'{beginn_lauteste_stunde.strftime("%H:00")} - {ende_lauteste_stunde.strftime("%H:00")}'
            curr_excel_ws.ws_cells[(i + 3, 8)].format.set_font_size(8)
            curr_excel_ws.ws_cells[(i + 3, 8)].format.set_align("center")
            curr_excel_ws.ws_cells[(i + 3, 11)].content = i+1
            curr_excel_ws.ws_cells[(i + 3, 11)].format.set_font_size(8)


        except KeyError as ex:
            logging.exception(ex)

    curr_excel_ws.write_to_workbook()
    line_chart = wb.add_chart({'type': 'line'})
    line_chart.set_title({'name': f"Beurteilungspegel {bezeichnung_worksheet}\n{ausgewerteter_monat}"})
    line_chart.add_series({"name": "Beurteilungspegel Tag", 'values': f"'{bezeichnung_worksheet}'!$D$4:$D${number_days_in_month+3}"})
    line_chart.add_series({"name": "Grenzwert Tag", 'values': f"='{bezeichnung_worksheet}'!$M$4:$M${number_days_in_month+3}"})
    line_chart.add_series({"name": "Beurteilungspegel Nacht", 'values': f"'{bezeichnung_worksheet}'!$F$4:$F${number_days_in_month+3}"})
    line_chart.add_series({"name": "Grenzwert Nacht", 'values': f"'{bezeichnung_worksheet}'!$N$4:$N${number_days_in_month+3}"})

    line_chart.set_size({'width': 625, 'height': 250})
    worksheet.insert_chart('B36', line_chart)
    
    red_bg_font_format = wb.add_format({'bg_color': '#FFC7CE',
                                'font_color': '#9C0006'})
    worksheet.conditional_format(3, 3, 3 + number_days_in_month - 1, 3, {
        'type':     'cell',
        'criteria': 'greater than',
        'value': '$M$4',
        'format':   red_bg_font_format})
    worksheet.conditional_format(3, 5, 3 + number_days_in_month - 1, 5, {
        'type': 'cell',
        'criteria': 'greater than',
        'value': '$N$4+0.5', # Prüfen, ob das funktioniert
        'format': red_bg_font_format})


    worksheet.print_area(0, 0, 50, 8)

def erstelle_sheet_schallleistungspegel(wb: xlsxwriter.Workbook, year: int, month: int, schallleistungspegel: typing.Dict[typing.Tuple[int, int], float]):
    my_sheetname = "Schallleistungspegel"
    worksheet = wb.add_worksheet(my_sheetname)
    locale.setlocale(locale.LC_TIME, 'German') #locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')

    first_col = 1
    first_row = 2

    worksheet.set_row(3 - first_row, 60)
    worksheet.set_column(2 - first_col, 3 - first_col, 20)
    worksheet.set_column(7 - first_col, 8 -first_col, 20)
    worksheet.set_column(9 - first_col, 33 -first_col, 8)

    cell_formats_curr_worksheet = {}
    first_of_month = dt.datetime(year, month, 1)
    number_days_in_month = monthrange(year, month)[1]
    for i in range(0, 40):
        for j in range(0, 40):
            cell_formats_curr_worksheet[(i, j)] = wb.add_format({})
    curr_excel_ws = ExcelWorksheet(i, j, wb, worksheet)

    curr_excel_ws.make_box_around_cells(2 - first_row, 4 - first_col, 3 - first_row, 5 - first_col)

    curr_excel_ws.merge_cells(2 - first_row, 4 - first_col, 2 - first_row, 5 - first_col)

    current_cell = (2 - first_row, 4 - first_col)

    curr_excel_ws.ws_cells[current_cell].content = f"Schallleistungpegel"
    curr_excel_ws.ws_cells[current_cell].format.set_bg_color(white_smoke)
    curr_excel_ws.ws_cells[current_cell].format.set_align("center")
    curr_excel_ws.ws_cells[current_cell].format.set_valign("vcenter")
    curr_excel_ws.ws_cells[current_cell].format.set_bold(True)

    current_cell = (3 - first_row, 4 - first_col)

    curr_excel_ws.ws_cells[current_cell].content = f"tags \n06:00-22:00"
    curr_excel_ws.ws_cells[current_cell].format.set_bg_color(white_smoke)
    curr_excel_ws.ws_cells[current_cell].format.set_align("center")
    curr_excel_ws.ws_cells[current_cell].format.set_valign("vcenter")
    curr_excel_ws.ws_cells[current_cell].format.set_text_wrap(True)
    curr_excel_ws.ws_cells[current_cell].format.set_font_size(8)
    curr_excel_ws.ws_cells[current_cell].format.set_bold(True)

    current_cell = (3 - first_row, 5 - first_col)
    curr_excel_ws.ws_cells[current_cell].content = f"nachts \n22:00-06:00"
    curr_excel_ws.ws_cells[current_cell].format.set_bg_color(white_smoke)
    curr_excel_ws.ws_cells[current_cell].format.set_align("center")
    curr_excel_ws.ws_cells[current_cell].format.set_valign("vcenter")
    curr_excel_ws.ws_cells[current_cell].format.set_text_wrap(True)
    curr_excel_ws.ws_cells[current_cell].format.set_font_size(8)
    curr_excel_ws.ws_cells[current_cell].format.set_bold(True)

    curr_excel_ws.make_box_around_cells(4 - first_row, 2 - first_col, 4+number_days_in_month-1 - first_row, 3 - first_col)
    curr_excel_ws.make_box_around_cells(4 - first_row, 4 - first_col, 4+number_days_in_month-1 - first_row, 5 - first_col)

    
    curr_excel_ws.merge_cells(3 - first_row, 7 - first_col, 3 - first_row, 8 - first_col)
    curr_excel_ws.make_box_around_cells(3 - first_row, 7 - first_col, 3 - first_row, 8 - first_col)

    current_cell = (3- first_row, 7 - first_col)

    curr_excel_ws.ws_cells[current_cell].content = f"Schallleistungspegel\nA-Bew.\n[dB(A)]"
    curr_excel_ws.ws_cells[current_cell].format.set_bg_color(white_smoke)
    curr_excel_ws.ws_cells[current_cell].format.set_align("center")
    curr_excel_ws.ws_cells[current_cell].format.set_valign("vcenter")
    curr_excel_ws.ws_cells[current_cell].format.set_text_wrap(True)
    curr_excel_ws.ws_cells[current_cell].format.set_bold(True)

    curr_excel_ws.make_box_around_cells(3 - first_row, 9 - first_col, 3 - first_row, 9+24-1 - first_col)
    for i in range(0, 24):
        current_cell = (3 - first_row, 9+i - first_col)
        curr_excel_ws.ws_cells[current_cell].content = f"{i:02d}:00\n-\n{i+1:02d}:00"
        curr_excel_ws.ws_cells[current_cell].format.set_bg_color(white_smoke)
        curr_excel_ws.ws_cells[current_cell].format.set_align("center")
        curr_excel_ws.ws_cells[current_cell].format.set_valign("vcenter")
        curr_excel_ws.ws_cells[current_cell].format.set_text_wrap(True)
        curr_excel_ws.ws_cells[current_cell].format.set_bold(True)
    offset = 4
    
    for i in range(0+offset, number_days_in_month + offset):
        dayname =(first_of_month + dt.timedelta(days=i)).strftime("%A")
        date_string = (first_of_month + dt.timedelta(days=i-offset)).strftime("%d.%m.%Y")
        for ii in [2, 7,]:
            current_cell = (i - first_row, ii - first_col)
            curr_excel_ws.ws_cells[current_cell].content = f"{dayname}"
            curr_excel_ws.ws_cells[current_cell].format.set_bg_color(white_smoke)
            curr_excel_ws.ws_cells[current_cell].format.set_align("center")
            curr_excel_ws.ws_cells[current_cell].format.set_valign("vcenter")
        for ii in [2+1, 7+1,]:
            current_cell = (i - first_row, ii - first_col)
            curr_excel_ws.ws_cells[current_cell].content = f"{date_string}"
            curr_excel_ws.ws_cells[current_cell].format.set_bg_color(white_smoke)
            curr_excel_ws.ws_cells[current_cell].format.set_align("center")
            curr_excel_ws.ws_cells[current_cell].format.set_valign("vcenter")

    curr_excel_ws.make_box_around_cells(4 - first_row, 7 - first_col, 4+number_days_in_month-1 - first_row, 32 - first_col)

    for i in range(0, 24):
        for ii in range(0, number_days_in_month):
            current_cell = (ii+offset - first_row, 9+i - first_col)
            try:
                
                curr_excel_ws.ws_cells[current_cell].content = schallleistungspegel[(ii+1, i)]
                curr_excel_ws.ws_cells[current_cell].format.set_num_format('0.0')
                if i in [r for r in range(0, 6)] + [r for r in range(22, 24)]:
                    curr_excel_ws.ws_cells[current_cell].format.set_bg_color(dark_gray)
            except KeyError as e:
                logging.error(e)


    for i in range(0, 2):
        for ii in range(0, number_days_in_month):
            current_cell = (ii+offset - first_row, 4+i - first_col)
            curr_excel_ws.ws_cells[current_cell].content = 15.8
            curr_excel_ws.ws_cells[current_cell].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[current_cell].is_formula = True
            start_tagzeitraum_cols = 15
            end_tagzeitraum_cols = start_tagzeitraum_cols + 5
            
            
            # 10*LOG(MITTELWERT(WENN(ISTZAHL($AB4:$AQ4);(10^($AB4:$AQ4/10)))));
            #curr_excel_ws.ws_cells[current_cell].content = f'{{10*LOG10(AVERAGE(IF($I${ii+offset-first_row}:$I${number_days_in_month+4-1}>0, 10^(0.1*$I$4:$I${number_days_in_month+4-1}))))}}'
            if i == 0:
                curr_excel_ws.ws_cells[current_cell].content = f"=10*LOG10(AVERAGE(10^(0.1*O{ii+offset - first_row + 1}:AD{ii+offset - first_row + 1})))"
            if i == 1:
                curr_excel_ws.ws_cells[current_cell].content = f"=MAX(I{ii+offset - first_row + 1}:N{ii+offset - first_row + 1}, AE{ii+offset - first_row + 1}:AF{ii+offset - first_row + 1})"
                curr_excel_ws.ws_cells[current_cell].format.set_bg_color(dark_gray)
            

    curr_excel_ws.write_to_workbook()

def create_content_worksheet_uebersicht(wb, worksheet, uebersichtsdaten: Monatsbericht, number_days_in_month: int, first_of_month: int):
        # Used global variables: first_of_month, number_days_in_month
        # Page Setup
        if True:
            worksheet.set_column(0, 0, 0)
            worksheet.set_column(1, 1, 30)
            worksheet.set_column(2, 2, 3)
            worksheet.set_column(3, 3, 15)
            worksheet.set_column(4, 4, 3)
            worksheet.set_column(5, 5, 15)
            worksheet.set_column(6, 6, 3)
            worksheet.set_column(7, 7, 15)
            worksheet.set_column(8, 8, 3)
            worksheet.set_column(9, 9, 15)
        worksheet.set_landscape()

        # worksheet.fit_to_pages(1, 1)
        # worksheet.hide_gridlines()
        # worksheet.print_area(0, 0, 1, 30)
        # worksheet.set_v_pagebreaks([20])
        worksheet.set_h_pagebreaks([35])
        worksheet.set_page_view()
        prozent_witterungsfilter = uebersichtsdaten.no_aussortiert_wetter/(number_days_in_month*24*3600)
        prozent_fremdgeraeuschfilter = uebersichtsdaten.no_aussortiert_sonstige/(number_days_in_month*24*3600)

        prozent_technische_ausfaelle = ((number_days_in_month*24*3600)-uebersichtsdaten.no_verwertbare_sekunden)/(number_days_in_month*24*3600)
        cell_formats_curr_worksheet = {}
        locale.setlocale(locale.LC_TIME, 'German') #locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
        ueberschrift = first_of_month.strftime("%B %Y")
        projektbezeichnung = uebersichtsdaten.projekt.name
        cells_cur_worksheet = {}
        for i in range(0, 40):
            for j in range(0, 40):
                cell_formats_curr_worksheet[(i, j)] = wb.add_format({})
        curr_excel_ws = ExcelWorksheet(i, j, wb, worksheet)

        curr_excel_ws.make_box_around_cells(0, 1, 2, 6)
        curr_excel_ws.ws_cells[(0, 1)].content = f"{ueberschrift}\n{projektbezeichnung}"
        curr_excel_ws.ws_cells[(0, 1)].format.set_bg_color(white_smoke)
        curr_excel_ws.ws_cells[(0, 1)].format.set_align("center")
        curr_excel_ws.ws_cells[(0, 1)].format.set_valign("vcenter")
        curr_excel_ws.ws_cells[(0, 1)].format.set_bold(True)

        curr_excel_ws.merge_cells(0, 1, 2, 6)

        curr_excel_ws.make_box_around_cells(5, 1, 6, 5)
        curr_excel_ws.ws_cells[(5, 1)].content = "Witterungsbedingte Ausfälle"
        curr_excel_ws.merge_cells(5, 1, 5, 4)
        curr_excel_ws.ws_cells[(5, 1)].format.set_bold(True)
        curr_excel_ws.ws_cells[(5, 1)].format.set_bg_color(silver)

        curr_excel_ws.ws_cells[(5, 5)].content = prozent_witterungsfilter*100
        curr_excel_ws.ws_cells[(5, 5)].format.set_num_format('0.0')

        curr_excel_ws.ws_cells[(6, 1)].content = "Bsp.: Starker Regen oder Wind"
        curr_excel_ws.merge_cells(6, 1, 6, 4)
        curr_excel_ws.ws_cells[(6, 1)].format.set_bg_color(silver)

        curr_excel_ws.make_box_around_cells(7, 1, 8, 5)
        curr_excel_ws.ws_cells[(7, 1)].content = "Fremdgeräuschbedingte Ausfälle"
        curr_excel_ws.merge_cells(7, 1, 7, 4)
        curr_excel_ws.ws_cells[(7, 1)].format.set_bold(True)
        curr_excel_ws.ws_cells[(7, 1)].format.set_bg_color(white_smoke)

        curr_excel_ws.ws_cells[(7, 5)].content = prozent_fremdgeraeuschfilter*100
        curr_excel_ws.ws_cells[(7, 5)].format.set_num_format('0.0')

        curr_excel_ws.ws_cells[(8, 1)].content = "Bsp.: Vogelgesang"
        curr_excel_ws.merge_cells(8, 1, 8, 4)
        curr_excel_ws.ws_cells[(8, 1)].format.set_bg_color(white_smoke)

        curr_excel_ws.make_box_around_cells(9, 1, 10, 5)
        curr_excel_ws.ws_cells[(9, 1)].content = "Technische Ausfälle"
        curr_excel_ws.merge_cells(9, 1, 9, 4)
        curr_excel_ws.ws_cells[(9, 1)].format.set_bold(True)
        curr_excel_ws.ws_cells[(9, 1)].format.set_bg_color(silver)

        curr_excel_ws.ws_cells[(9, 5)].content = prozent_technische_ausfaelle*100
        curr_excel_ws.ws_cells[(9, 5)].format.set_num_format('0.0')

        curr_excel_ws.merge_cells(10, 1, 10, 4)
        curr_excel_ws.ws_cells[(10, 1)].format.set_bg_color(silver)

        curr_excel_ws.make_box_around_cells(11, 1, 12, 5)
        curr_excel_ws.ws_cells[(11, 1)].content = "auswertbare Messwerte %"
        curr_excel_ws.merge_cells(11, 1, 11, 4)
        curr_excel_ws.ws_cells[(11, 1)].format.set_bold(True)
        curr_excel_ws.ws_cells[(11, 1)].format.set_bg_color(white_smoke)

        curr_excel_ws.ws_cells[(11, 5)].is_formula = True
        curr_excel_ws.ws_cells[(11, 5)].content = "=100-F10-F8-F6"
        curr_excel_ws.ws_cells[(11, 5)].format.set_num_format('0.0')

        curr_excel_ws.merge_cells(12, 1, 12, 5)
        curr_excel_ws.ws_cells[(12, 1)].format.set_bg_color(white_smoke)

        curr_excel_ws.make_box_around_cells(15, 3, 15, 9)
        curr_excel_ws.ws_cells[(15, 3)].content = "TA Lärm"
        curr_excel_ws.merge_cells(15, 3, 15, 9)
        curr_excel_ws.ws_cells[(15, 3)].format.set_bold(True)
        curr_excel_ws.ws_cells[(15, 3)].format.set_bg_color(gray)
        curr_excel_ws.ws_cells[(15, 3)].format.set_align("center")
        curr_excel_ws.ws_cells[(15, 3)].format.set_valign("vcenter")

        curr_excel_ws.make_box_around_cells(17, 3, 18, 5, 3)
        curr_excel_ws.ws_cells[(17, 3)].content = "Mittlerer\nBeurteilungspegel"
        curr_excel_ws.merge_cells(17, 3, 18, 5)
        curr_excel_ws.ws_cells[(17, 3)].format.set_bg_color(gray)
        curr_excel_ws.ws_cells[(17, 3)].format.set_align("center")
        curr_excel_ws.ws_cells[(17, 3)].format.set_valign("vcenter")

        curr_excel_ws.make_box_around_cells(17, 7, 18, 9, 3)
        curr_excel_ws.ws_cells[(17, 7)].content = "Immissions-\nrichtwerte"
        curr_excel_ws.merge_cells(17, 7, 18, 9)
        curr_excel_ws.ws_cells[(17, 7)].format.set_bg_color(gray)
        curr_excel_ws.ws_cells[(17, 7)].format.set_align("center")
        curr_excel_ws.ws_cells[(17, 7)].format.set_valign("vcenter")

        curr_excel_ws.make_box_around_cells(19, 1, 19, 1, 2)
        curr_excel_ws.ws_cells[(19, 1)].content = "Aufpunkte"
        curr_excel_ws.ws_cells[(19, 1)].format.set_bg_color(gray)
        curr_excel_ws.ws_cells[(19, 1)].format.set_align("center")
        curr_excel_ws.ws_cells[(19, 1)].format.set_valign("vcenter")
        curr_excel_ws.ws_cells[(19, 1)].format.set_bold(True)

        curr_excel_ws.make_box_around_cells(19, 3, 20, 3, 1)
        curr_excel_ws.ws_cells[(19, 3)].content = "Tag 06 - 22h"
        curr_excel_ws.ws_cells[(19, 3)].format.set_align("center")
        curr_excel_ws.ws_cells[(20, 3)].content = "Kl = 0"
        curr_excel_ws.ws_cells[(20, 3)].format.set_align("center")

        curr_excel_ws.make_box_around_cells(19, 5, 20, 5, 1)
        curr_excel_ws.ws_cells[(19, 5)].content = "Nacht 22 - 06h"
        curr_excel_ws.ws_cells[(19, 5)].format.set_align("center")
        curr_excel_ws.ws_cells[(20, 5)].content = "Kl = 0"
        curr_excel_ws.ws_cells[(20, 5)].format.set_align("center")

        curr_excel_ws.make_box_around_cells(19, 7, 20, 7, 1)
        curr_excel_ws.ws_cells[(19, 7)].content = "Tag"
        curr_excel_ws.ws_cells[(19, 7)].format.set_align("center")
        curr_excel_ws.ws_cells[(20, 7)].content = " 06 - 22h"
        curr_excel_ws.ws_cells[(20, 7)].format.set_align("center")

        curr_excel_ws.make_box_around_cells(19, 9, 20, 9, 1)
        curr_excel_ws.ws_cells[(19, 9)].content = "Nacht"
        curr_excel_ws.ws_cells[(19, 9)].format.set_align("center")
        curr_excel_ws.ws_cells[(20, 9)].content = "22 - 06h"
        curr_excel_ws.ws_cells[(20, 9)].format.set_align("center")

        curr_excel_ws.make_box_around_cells(21, 3, 21 + len(uebersichtsdaten.details_io.keys()) - 1, 3, 1)
        curr_excel_ws.make_box_around_cells(21, 5, 21 + len(uebersichtsdaten.details_io.keys()) - 1, 5, 1)
        curr_excel_ws.make_box_around_cells(21, 7, 21 + len(uebersichtsdaten.details_io.keys()) - 1, 7, 1)
        curr_excel_ws.make_box_around_cells(21, 9, 21 + len(uebersichtsdaten.details_io.keys()) - 1, 9, 1)

        curr_excel_ws.make_box_around_cells(21, 1, 21 + len(uebersichtsdaten.details_io.keys()) - 1, 1)
        for index, key in enumerate(uebersichtsdaten.details_io.keys()):
            io = uebersichtsdaten.details_io[key]
            name_io = io.immissionsort.name_4_excel
            curr_excel_ws.ws_cells[(21+index, 1)].content = name_io #"Bachzimmererstr. 32"
           
            curr_excel_ws.ws_cells[(21 + index, 3)].is_formula = True
            curr_excel_ws.ws_cells[(21 + index, 3)].content = f'{{10*LOG10(AVERAGE(IF(\'{name_io}\'!$D$4:$D${number_days_in_month+4-1}>0, 10^(0.1*\'{name_io}\'!$D$4:$D${number_days_in_month+4-1}))))}}'
            # Führt zu Fehlern: curr_excel_ws.ws_cells[(21 + index, 3)].content = "10*LOG10(AVERAGE(IF(" + f"'{name_io}'" + f"!$D$4:$D${number_days_in_month+4-1}>0," + f"10^(0.1*'{name_io}'" + f"!$D$4:$D${number_days_in_month+4-1}))))"
            curr_excel_ws.ws_cells[(21 + index, 3)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(21 + index, 3)].format.set_align("center")

            curr_excel_ws.ws_cells[(21 + index, 5)].is_formula = True
            curr_excel_ws.ws_cells[(21 + index, 5)].content = f'{{10*LOG10(AVERAGE(IF(\'{name_io}\'!$F$4:$F${number_days_in_month+4-1}>0,10^(0.1*\'{name_io}\'!$F$4:$F${number_days_in_month+4-1}))))}}'
            # curr_excel_ws.ws_cells[(21 + index, 5)].content = "10*LOG10(AVERAGE(IF(" + f"'{name_io}'" + f"!$F$4:$F${number_days_in_month+4-1}>0," + f"10^(0.1*'{name_io}'" + f"!$F$4:$F${number_days_in_month+4-1}))))"
            curr_excel_ws.ws_cells[(21 + index, 5)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(21 + index, 5)].format.set_align("center")

            curr_excel_ws.ws_cells[(21 + index, 7)].is_formula = True
            curr_excel_ws.ws_cells[(21 + index, 7)].content = f"'{name_io}'!$M$4"
            curr_excel_ws.ws_cells[(21 + index, 7)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(21 + index, 7)].format.set_align("center")

            curr_excel_ws.ws_cells[(21 + index, 9)].is_formula = True
            curr_excel_ws.ws_cells[(21 + index, 9)].content = f"'{name_io}'!$N$4"
            curr_excel_ws.ws_cells[(21 + index, 9)].format.set_num_format('0.0')
            curr_excel_ws.ws_cells[(21 + index, 9)].format.set_align("center")
        # curr_excel_ws.ws_cells[(22, 1)].content = "Ziegelhütte 4"
            if index % 2 == 0:
                curr_excel_ws.ws_cells[(21+index, 1)].format.set_bg_color(white_smoke)
                curr_excel_ws.ws_cells[(21 + index, 3)].format.set_bg_color(white_smoke)
                curr_excel_ws.ws_cells[(21 + index, 5)].format.set_bg_color(white_smoke)
                curr_excel_ws.ws_cells[(21 + index, 7)].format.set_bg_color(white_smoke)
                curr_excel_ws.ws_cells[(21 + index, 9)].format.set_bg_color(white_smoke)
        # curr_excel_ws.ws_cells[(23, 1)].content = "Kreutzerweg 4"
        # curr_excel_ws.ws_cells[(24, 1)].content = "Am Hewenegg 1"
        # curr_excel_ws.ws_cells[(24, 1)].format.set_bg_color(white_smoke)
        # curr_excel_ws.ws_cells[(25, 1)].content = "Am Hewenegg 8"
        curr_excel_ws.write_to_workbook()

        pie_chart = wb.add_chart({'type': 'pie'})
        pie_chart.add_series({
            'name':       'Messwerterfassung',
            'categories': "=(Monat!$B$6:$B$6,Monat!$B$8:$B$8,Monat!$B$10:$B$10,Monat!$B$12:$B$12)",
            'values':     "=(Monat!$F$6:$F$6,Monat!$F$8:$F$8,Monat!$F$10:$F$10,Monat!$F$12:$F$12)",
        })
        pie_chart.set_size({'width': 360, 'height': 250})
        worksheet.insert_chart('H2', pie_chart)

class ExcelWorksheet:
    def __init__(self, no_rows, no_cols, wb, ws):
        self.ws_cells = {}
        for i in range(0, no_rows):
            for j in range(0, no_cols):
                self.ws_cells[(i, j)] = ExcelCell(i, j, wb, ws)

    def merge_cells(self, from_r, from_c, to_r, to_c):
        pop_all_but_one = True
        for i in [j for j in self.ws_cells if
                from_r <= j[0] <= to_r
                and to_c >= j[1] >= from_c
                ]:
            if pop_all_but_one:
                first = i
                pop_all_but_one = False
                self.ws_cells[i].format.set_text_wrap(True)
                self.ws_cells[i].format.set_align("center")
                # self.ws_cells[i].format.set_top(2)
                # self.ws_cells[i].format.set_left(2)
                # self.ws_cells[i].format.set_right(2)
                # self.ws_cells[i].format.set_bottom(2)
                self.ws_cells[i].is_merged = True
                self.ws_cells[i].merging_information = {
                    "from_r": from_r,
                    "from_c": from_c,
                    "to_r": to_r,
                    "to_c": to_c
                }
            else:
                if self.ws_cells[i].format.top != 0:
                    self.ws_cells[first].format.set_top(self.ws_cells[i].format.top)
                    logging.debug(f"found top at {i}")
                if self.ws_cells[i].format.left != 0:
                    logging.debug(f"found left at {i}")
                    self.ws_cells[first].format.set_left(self.ws_cells[i].format.left)
                if self.ws_cells[i].format.bottom != 0:
                    self.ws_cells[first].format.set_bottom(self.ws_cells[i].format.bottom)
                    logging.debug(f"found bottom at {i}")
                if self.ws_cells[i].format.right != 0:
                    logging.debug(f"found right at {i}")
                    self.ws_cells[first].format.set_right(self.ws_cells[i].format.right)
                self.ws_cells.pop(i)

    def make_box_around_cells(self, from_r, from_c, to_r, to_c, boxtype = 2, bg_color = None):
        for i in [j for j in self.ws_cells if
                from_r <= j[0] <= to_r
                and to_c >= j[1] >= from_c and j[1]
                ]:
            if bg_color is not None:
                self.ws_cells[i].format.set_bg_color(bg_color)
            pass
            # self.ws_cells[i].format.set_bg_color('red')
        for i in [j for j in self.ws_cells if
                from_r <= j[0] <= to_r
                and from_c == j[1]
                ]:
            self.ws_cells[i].format.set_left(boxtype)

        for i in [j for j in self.ws_cells if
                from_r <= j[0] <= to_r
                and to_c == j[1]
                ]:
            self.ws_cells[i].format.set_right(boxtype)

        for i in [j for j in self.ws_cells if
                from_r == j[0]
                and to_c >= j[1] >= from_c and j[1]
                ]:
            self.ws_cells[i].format.set_top(boxtype)

        for i in [j for j in self.ws_cells if
                j[0] == to_r
                and to_c >= j[1] >= from_c and j[1]
                ]:
            self.ws_cells[i].format.set_bottom(boxtype)

    def write_to_workbook(self):
        logging.debug("Writing content")
        for i in self.ws_cells:
            self.ws_cells[i].write_to_worksheet()


class ExcelCell:
    def __init__(self, row, col, wb, ws):
        self.row = row
        self.col = col
        self.format = wb.add_format({})
        self.content = None
        self.worksheet = ws
        self.is_merged = False
        self.merging_information = None
        self.is_formula = False

    def write_to_worksheet(self):
        if self.is_merged:
            self.worksheet.merge_range(
                self.merging_information["from_r"],
                self.merging_information["from_c"],
                self.merging_information["to_r"],
                self.merging_information["to_c"],
                self.content, self.format)
        elif self.is_formula:
            self.worksheet.write_formula(self.row, self.col, self.content, self.format)
        else:
            self.worksheet.write(self.row, self.col, self.content, self.format)


    