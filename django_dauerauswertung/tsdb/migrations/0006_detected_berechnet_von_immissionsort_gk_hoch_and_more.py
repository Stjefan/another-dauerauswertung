# Generated by Django 4.1.2 on 2022-10-13 14:51

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('tsdb', '0005_remove_rejection_messpunkt_rejected_messpunkt'),
    ]

    operations = [
        migrations.AddField(
            model_name='detected',
            name='berechnet_von',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='tsdb.auswertungslauf'),
        ),
        migrations.AddField(
            model_name='immissionsort',
            name='gk_hoch',
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name='immissionsort',
            name='gk_rechts',
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name='lrpegel',
            name='berechnet_von',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='tsdb.auswertungslauf'),
        ),
        migrations.AddField(
            model_name='maxpegel',
            name='berechnet_von',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='tsdb.auswertungslauf'),
        ),
        migrations.AddField(
            model_name='messpunkt',
            name='gk_hoch',
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name='messpunkt',
            name='gk_rechts',
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name='messpunkt',
            name='is_meteo_station',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='rejected',
            name='berechnet_von',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='tsdb.auswertungslauf'),
        ),
        migrations.AddField(
            model_name='schallleistungpegel',
            name='berechnet_von',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='tsdb.auswertungslauf'),
        ),
    ]
