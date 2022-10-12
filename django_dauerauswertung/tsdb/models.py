from email.policy import default
from django.db import models

# Create your models here.
from timescale.db.models.models import TimescaleModel, TimescaleDateTimeField

class Projekt(models.Model):
    name = models.CharField(max_length = 200)

class Messpunkt(models.Model):
    name = models.CharField(max_length = 200)
    projekt = models.ForeignKey(Projekt, on_delete=models.CASCADE, null=True, blank=True)

class Immissionsort(models.Model):
    name = models.CharField(max_length = 200)
    projekt = models.ForeignKey(Projekt, on_delete=models.CASCADE, null=True, blank=True)
    grenzwert_tag = models.FloatField(default=0.0)
    grenzwert_nacht = models.FloatField(default=0.0)


class Schallursache(models.Model):
    name = models.CharField(max_length = 200)
    gemessen_an = models.ForeignKey(Messpunkt, on_delete=models.CASCADE, null=True, blank=True)

class Rejection(models.Model):
    name = models.CharField(max_length = 200)
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.CASCADE, null=True, blank=True)

class Detection(models.Model):
    name = models.CharField(max_length = 200)

class ExecutedEvaluation(models.Model):
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



class LrPegel(TimescaleModel):
    immissionsort = models.ForeignKey(Immissionsort, on_delete=models.CASCADE, null=True, blank=True)
    verursacht = models.ForeignKey(Schallursache, on_delete=models.CASCADE, null=True, blank=True)
    pegel = models.FloatField()

class MaxPegel(TimescaleModel):
    immissionsort = models.ForeignKey(Immissionsort, on_delete=models.CASCADE, null=True, blank=True)
    pegel = models.FloatField()

class SchallleistungPegel(TimescaleModel):
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.CASCADE, null=True, blank=True)
    pegel = models.FloatField()

class Detected(TimescaleModel):
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.SET_NULL, null=True, blank=True)
    dauer = models.FloatField()
    typ = models.ForeignKey(Detection, on_delete=models.CASCADE, null=True, blank=True)

class Rejected(TimescaleModel):
    filter = models.ForeignKey(Rejection, on_delete=models.CASCADE, null=True, blank=True)

class EvaluationMesspunkt(models.Model):
    time = models.DateTimeField()
    lafeq = models.FloatField()
    rejected = models.FloatField(null=True)
    detected = models.FloatField(null=True)
    messpunkt = models.ForeignKey(Messpunkt, on_delete=models.DO_NOTHING)
    
    class Meta:
        managed = False
        db_table = 'tsdb_evaluationmesspunkt'
        
        # CREATE VIEW tsdb_evaluationmesspunkt_v6 AS SELECT r.id as id, r.time as time, r.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE r.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE r.lafeq END as rejected, r.messpunkt_id as messpunkt_id FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time = rej.time;

        # SELECT * FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) WHERE r.time >= '2022-10-05 05:00:25' and r.time <= '2022-10-05 05:00:50';

        # SELECT * FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time = rej.time WHERE r.time >= '2022-10-05 05:00:25' and r.time <= '2022-10-05 05:00:50';

        # SELECT r.time as time, r.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE r.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE r.lafeq END as rejected, r.messpunkt_id as messpunkt_id FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time = rej.time WHERE r.time >= '2022-10-05 05:00:25' and r.time <= '2022-10-05 05:00:50';

        # ASOF
        # SELECT r.time as time, r.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE r.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE r.lafeq END as rejected, r.messpunkt_id as messpunkt_id FROM tsdb_resu r LEFT JOIN tsdb_detected d ON r.time >= d.time AND r.time <= (d.time + (INTERVAL '1 sec' * d.dauer)) LEFT JOIN tsdb_rejected rej ON r.time >= rej.time and r.time <= (rej.time + (INTERVAL '1 sec')) WHERE r.time >= '2022-12-01 10:00:05' and r.time <= '2022-12-01 10:00:50';

