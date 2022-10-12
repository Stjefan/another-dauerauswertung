# Generated by Django 4.1.2 on 2022-10-12 17:42

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('tsdb', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='executedevaluation',
            name='zuordnung',
            field=models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to='tsdb.projekt'),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='immissionsort',
            name='grenzwert_nacht',
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name='immissionsort',
            name='grenzwert_tag',
            field=models.FloatField(default=0.0),
        ),
    ]