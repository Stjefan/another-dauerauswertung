
from datetime import datetime
from django.db import models
import logging

# Create your models here.
from timescale.db.models.models import TimescaleModel, TimescaleDateTimeField

class Projekt(models.Model):
    name = models.CharField(max_length = 200)
    utm_x = models.FloatField(default=0.0)
    utm_y = models.FloatField(default=0.0)

    name_monatsbericht_excel = models.CharField(max_length = 200, default="N_A")



class Messpunkt(models.Model):
    name = models.CharField(max_length = 200)
    projekt = models.ForeignKey(Projekt, on_delete=models.CASCADE)
    gk_rechts = models.FloatField(default=0.0)
    gk_hoch = models.FloatField(default=0.0)

    utm_x = models.FloatField(default=0.0)
    utm_y = models.FloatField(default=0.0)

    is_meteo_station = models.BooleanField(default=False)
    id_external = models.IntegerField(default=0)
    upload_folder_svantek_file = models.CharField(max_length = 200, null=True, blank=True)
    ablage_folder_transmes = models.CharField(max_length = 200, null=True, blank=True)

    seriennummer_messstation = models.CharField(max_length = 200, null=True, blank=True)
    lwa = models.FloatField(default=0.0)

class Immissionsort(models.Model):
    name = models.CharField(max_length = 200)
    projekt = models.ForeignKey(Projekt, on_delete=models.CASCADE)
    grenzwert_tag = models.FloatField(default=0.0)
    grenzwert_nacht = models.FloatField(default=0.0)
    gk_rechts = models.FloatField(default=0.0)
    gk_hoch = models.FloatField(default=0.0)

    utm_x = models.FloatField(default=0.0)
    utm_y = models.FloatField(default=0.0)

    name_4_excel =  models.CharField(max_length = 32)
    id_external = models.IntegerField(default=0)


class LaermursacheAnMesspunkt(models.Model):
    name = models.CharField(max_length = 200)
    gemessen_an = models.ForeignKey(Messpunkt, on_delete=models.CASCADE, null=True, blank=True)

class LaermursacheAnImmissionsorten(models.Model):
    name = models.CharField(max_length = 200)
    projekt = models.ForeignKey(Projekt, on_delete=models.CASCADE, null=True, blank=True)

class Ausbreitstungsfaktor(models.Model):
    immissionsort = models.ForeignKey(Immissionsort, on_delete=models.CASCADE, null=True, blank=True)
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.CASCADE, null=True, blank=True)
    ausbreitungskorrektur = models.FloatField(default=0.0)

class Rejection(models.Model):
    name = models.CharField(max_length = 200)

class Detection(models.Model):
    name = models.CharField(max_length = 200)

class Auswertungslauf(models.Model):
    zeitpunkt_im_beurteilungszeitraum = models.DateTimeField()
    zeitpunkt_durchfuehrung = models.DateTimeField()
    verhandene_messwerte = models.IntegerField()
    verwertebare_messwerte = models.IntegerField()
    in_berechnung_gewertete_messwerte =  models.IntegerField()
    zuordnung = models.ForeignKey(Projekt, on_delete=models.CASCADE)


class Resu(TimescaleModel):

    messpunkt = models.ForeignKey(
        Messpunkt, on_delete=models.CASCADE
    )
    lafeq = models.FloatField(default=0.0)
    lcfeq = models.FloatField(default=0.0)
    lafmax = models.FloatField(default=0.0)
    class Meta:
        indexes = [
                models.Index(fields=['time', 'messpunkt'], name='resu_time_messpunkt_idx'),
            ]


class Terz(TimescaleModel):

    messpunkt = models.ForeignKey(
        Messpunkt, on_delete=models.CASCADE
    )

    hz20 = models.FloatField(default=0.0)
    hz25 = models.FloatField(default=0.0)
    hz31_5 = models.FloatField(default=0.0)
    hz40 = models.FloatField(default=0.0)
    hz50 = models.FloatField(default=0.0)
    hz63 = models.FloatField(default=0.0)
    hz80 = models.FloatField(default=0.0)
    hz100 = models.FloatField(default=0.0)
    hz125 = models.FloatField(default=0.0)
    hz160 = models.FloatField(default=0.0)
    hz200 = models.FloatField(default=0.0)
    hz250 = models.FloatField(default=0.0)
    hz315 = models.FloatField(default=0.0)
    hz400 = models.FloatField(default=0.0)
    hz500 = models.FloatField(default=0.0)
    hz630 = models.FloatField(default=0.0)
    hz800 = models.FloatField(default=0.0)
    hz1000 = models.FloatField(default=0.0)
    hz1250 = models.FloatField(default=0.0)
    hz1600 = models.FloatField(default=0.0)
    hz2000 = models.FloatField(default=0.0)
    hz2500 = models.FloatField(default=0.0)
    hz3150 = models.FloatField(default=0.0)
    hz4000 = models.FloatField(default=0.0)
    hz5000 = models.FloatField(default=0.0)
    hz6300 = models.FloatField(default=0.0)
    hz8000 = models.FloatField(default=0.0)
    hz10000 = models.FloatField(default=0.0)
    hz12500 = models.FloatField(default=0.0)
    hz16000 = models.FloatField(default=0.0)
    hz20000 = models.FloatField(default=0.0)


    class Meta:
        indexes = [
                models.Index(fields=['time', 'messpunkt'], name='terz_time_messpunkt_idx'),
            ]


class Mete(TimescaleModel):

    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.CASCADE, null=True, blank=True)

    rain = models.FloatField(default=0.0)
    temperature = models.FloatField(default=0.0)
    windspeed = models.FloatField(default=0.0)
    pressure = models.FloatField(default=0.0)
    humidity = models.FloatField(default=0.0)
    winddirection = models.FloatField(default=0.0)

    indexes = [
    ]

    class Meta:
        indexes = [
                models.Index(fields=['time', 'messpunkt'], name='mete_time_messpunkt_idx'),
            ]



class LrPegel(models.Model):
    time = models.DateTimeField()
    immissionsort = models.ForeignKey(Immissionsort, on_delete=models.CASCADE, null=True, blank=True)
    verursacht = models.ForeignKey(LaermursacheAnImmissionsorten, on_delete=models.CASCADE, null=True, blank=True)
    pegel = models.FloatField()
    berechnet_von = models.ForeignKey(Auswertungslauf, on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'tsdb_lrpegel'

class MaxPegel(models.Model):
    time = models.DateTimeField()
    immissionsort = models.ForeignKey(Immissionsort, on_delete=models.CASCADE, null=True, blank=True)
    pegel = models.FloatField()
    berechnet_von = models.ForeignKey(Auswertungslauf, on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'tsdb_maxpegel'

class SchallleistungPegel(models.Model):
    time = models.DateTimeField()
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.CASCADE, null=True, blank=True)
    pegel = models.FloatField()
    berechnet_von = models.ForeignKey(Auswertungslauf, on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'tsdb_schallleistungpegel'

class Detected(models.Model):
    time = models.DateTimeField()
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.SET_NULL, null=True, blank=True)
    dauer = models.FloatField()
    typ = models.ForeignKey(Detection, on_delete=models.CASCADE, null=True, blank=True)
    berechnet_von = models.ForeignKey(Auswertungslauf, on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'tsdb_detected'

class Rejected(models.Model):
    time = models.DateTimeField()
    filter = models.ForeignKey(Rejection, on_delete=models.CASCADE, null=True, blank=True)
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.SET_NULL, null=True, blank=True)
    berechnet_von = models.ForeignKey(Auswertungslauf, on_delete=models.CASCADE, null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'tsdb_rejected'

class EvaluationMesspunkt(models.Model):
    time = models.DateTimeField()
    lafeq = models.FloatField()
    rejected = models.FloatField(null=True)
    detected = models.FloatField(null=True)
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.DO_NOTHING)
    
    class Meta:
        managed = False
        db_table = 'tsdb_evaluationmesspunkt'
        # Select * from tsdb_detected d JOIN tsdb_messpunkt m ON d.messpunkt_id = m.id
        # Select * from tsdb_rejected r JOIN tsdb_messpunkt m ON r.messpunkt_id = m.id AS
        # Select * from tsdb_resu r JOIN tsdb_messpunkt ON r.messpunkt_id = m.id

        # SELECT r.id as id, r.time as time, r.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE r.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE r.lafeq END as rejected, r.messpunkt_id as messpunkt_id FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time = rej.time;

        # CREATE VIEW tsdb_evaluationmesspunkt_v6 AS SELECT r.id as id, r.time as time, r.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE r.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE r.lafeq END as rejected, r.messpunkt_id as messpunkt_id FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time = rej.time;

        # SELECT * FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) WHERE r.time >= '2022-10-05 05:00:25' and r.time <= '2022-10-05 05:00:50';

        # SELECT * FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time = rej.time WHERE r.time >= '2022-10-05 05:00:25' and r.time <= '2022-10-05 05:00:50';

        # SELECT r.time as time, r.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE r.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE r.lafeq END as rejected, r.messpunkt_id as messpunkt_id FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time = rej.time WHERE r.time >= '2022-10-05 05:00:25' and r.time <= '2022-10-05 05:00:50';

        # ASOF
        # SELECT r.time as time, r.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE r.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE r.lafeq END as rejected, r.messpunkt_id as messpunkt_id FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time >= rej.time and r.time <= (rej.time + (INTERVAL '1 sec')) WHERE r.time >= '2022-12-01 10:00:05' and r.time <= '2022-12-01 10:00:50';

