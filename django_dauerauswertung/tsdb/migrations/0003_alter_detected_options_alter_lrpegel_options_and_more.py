# Generated by Django 4.1.2 on 2022-10-28 08:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('tsdb', '0002_rejected_time'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='detected',
            options={'managed': False},
        ),
        migrations.AlterModelOptions(
            name='lrpegel',
            options={'managed': False},
        ),
        migrations.AlterModelOptions(
            name='maxpegel',
            options={'managed': False},
        ),
        migrations.AlterModelOptions(
            name='rejected',
            options={'managed': False},
        ),
        migrations.AlterModelOptions(
            name='schallleistungpegel',
            options={'managed': False},
        ),
        migrations.AddField(
            model_name='messpunkt',
            name='utm_x',
            field=models.FloatField(default=0.0),
        ),
        migrations.AddField(
            model_name='messpunkt',
            name='utm_y',
            field=models.FloatField(default=0.0),
        ),
    ]