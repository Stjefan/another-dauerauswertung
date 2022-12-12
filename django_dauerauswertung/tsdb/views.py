from django.shortcuts import render

from rest_framework import viewsets
from rest_framework import serializers

from rest_framework.views import APIView
from rest_framework.response import Response

from .models import (Ausbreitstungsfaktor, EvaluationMesspunkt, Immissionsort, Messpunkt,
Detection, Rejection,
LrPegel, SchallleistungPegel, MaxPegel, Detected, Rejected, 
Projekt, Resu, Terz, Mete, LaermursacheAnImmissionsorten, LaermursacheAnMesspunkt, Auswertungslauf)
from django.urls import path, include
from django_filters import rest_framework as filters
from django_filters import FilterSet, DateTimeFromToRangeFilter
from rest_framework.pagination import PageNumberPagination
from drf_writable_nested.serializers import WritableNestedModelSerializer
from django.db import transaction
import logging
from django.db import connection
from django.utils.datastructures import MultiValueDictKeyError
from django.utils.dateparse import parse_datetime


logger = logging.getLogger(__name__)

class SchallleistungPegelSerializer(serializers.ModelSerializer):
    class Meta:
        model = SchallleistungPegel
        fields = ["time", "pegel", "messpunkt"] # "__all__"
        
class MaxPegelSerializer(serializers.ModelSerializer):
    class Meta:
        model = MaxPegel
        fields = ["time", "pegel", "immissionsort"] # "__all__"

class RejectedSerializer(serializers.ModelSerializer):
    filter = serializers.SlugRelatedField(slug_field="name", read_only=True)
    class Meta:
        model = Rejected
        fields = ["time", "filter"] #"__all__"
                         
class DetectedSerializer(serializers.ModelSerializer):
    class Meta:
        model = Detected
        fields = ["time", 'dauer']


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 1000

# Create your views here.
class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = EvaluationMesspunkt
        fields = ['time', 'lafeq', 'rejected', 'detected']


class LaermursacheAnImmissionsortenSerializer(serializers.ModelSerializer):
    class Meta:
        model = LaermursacheAnImmissionsorten
        fields = "__all__"

        


class LaermursacheAnMesspunktSerializer(serializers.ModelSerializer):
    class Meta:
        model = LaermursacheAnMesspunkt
        fields = ["name", "id"]


class LrSerializer(serializers.ModelSerializer):
    # immissionsort = serializers.SlugRelatedField(slug_field="id_external", read_only=True)
    class Meta:
        model = LrPegel
        fields = ['time', 'pegel', 'verursacht', 'immissionsort', "id"]



class MessspunktSerializer(serializers.ModelSerializer):
    laermursacheanmesspunkt_set = LaermursacheAnMesspunktSerializer(many=True)
    class Meta:
        model = Messpunkt
        fields = ["name", "gk_rechts", "gk_hoch", "id", "is_meteo_station", "laermursacheanmesspunkt_set", "id_external", "lwa", "utm_x", "utm_y"]

class ImmissionsortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Immissionsort
        fields = ["name", "grenzwert_tag", "grenzwert_nacht", "gk_rechts", "gk_hoch", "id_external", "id", "utm_x", "utm_y"]

class ProjektFilter(FilterSet):
    class Meta:
        model = Projekt
        fields = {"name": ['exact', 'icontains'],
        "id": ['exact']
        }


class AusbreitungsfaktorSerializer(serializers.ModelSerializer):
    messpunkt = serializers.SlugRelatedField(slug_field="id_external", read_only=True)
    immissionsort = serializers.SlugRelatedField(slug_field="id_external", read_only=True)
    class Meta:
        model = Ausbreitstungsfaktor
        fields = ["messpunkt", "immissionsort", "ausbreitungskorrektur"]


class RejectionSerializer(serializers.ModelSerializer):

    class Meta:
        model = Rejection
        fields = ["name", "id"]


class ProjektSerializer(serializers.ModelSerializer):
    messpunkt_set = MessspunktSerializer(many=True)
    immissionsort_set = ImmissionsortSerializer(many=True)
    laermursacheanimmissionsorten_set = LaermursacheAnImmissionsortenSerializer(many=True)

    ausbreitungsfaktoren_set = serializers.SerializerMethodField()

    def get_ausbreitungsfaktoren_set(self, obj):
        return [
        AusbreitungsfaktorSerializer(abf).data 
        for mp in obj.messpunkt_set.all() for abf in mp.ausbreitstungsfaktor_set.all()]

    rejections = serializers.SerializerMethodField()

    def get_rejections(self, obj):
        return [
        RejectionSerializer(r).data for r in Rejection.objects.all()]
    class Meta:
        model = Projekt
        fields = ["name", "id", "messpunkt_set", "immissionsort_set", "laermursacheanimmissionsorten_set", "ausbreitungsfaktoren_set", "rejections", "utm_x", "utm_y"]

class LrFilter(FilterSet):
    time = DateTimeFromToRangeFilter()
    class Meta:
        model = LrPegel
        fields = {
            'time': [],
            'verursacht': ['exact'],
            'immissionsort': ['exact']
        }

# ViewSets define the view behavior.

class AuswertungMesspunktFilter(FilterSet):
    time = DateTimeFromToRangeFilter()
    class Meta:
        model = EvaluationMesspunkt
        fields = {
            'time': [],
            'messpunkt': ['exact']
        }

class UserViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = EvaluationMesspunkt.objects.all()
    serializer_class = UserSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = AuswertungMesspunktFilter

class TerzSerializer(serializers.ModelSerializer):
    class Meta:
        model = Terz
        fields = "__all__" #['time', 'hz20', 'hz31_5']

class ResuSerializer(serializers.ModelSerializer):
    class Meta:
        model = Resu
        fields = ['time', 'lafeq', 'lafmax']

class MeteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Mete
        fields = ['time', 
        'rain',
        'temperature', 
        'windspeed',
        'pressure',
        'humidity',
        'winddirection',
        ]

class ResuFilter(FilterSet):
    time = DateTimeFromToRangeFilter()
    class Meta:
        model = Resu
        fields = {
            'time': [],
            'messpunkt': ['exact']
        }

class MeteFilter(FilterSet):
    time = DateTimeFromToRangeFilter()
    class Meta:
        model = Mete
        fields = {
            'time': [],
            'messpunkt': ['exact']
        }

class TerzFilter(FilterSet):
    time = DateTimeFromToRangeFilter()
    class Meta:
        model = Terz
        fields = {
            'time': [],
            'messpunkt': ['exact']
        }

class ResuViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Resu.objects.all().order_by('time')
    serializer_class = ResuSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = ResuFilter
    # pagination_class = StandardResultsSetPagination


class TerzViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Terz.objects.all().order_by('time')
    serializer_class = TerzSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = TerzFilter
    # pagination_class = StandardResultsSetPagination

class MeteViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Mete.objects.filter(time__second=0).order_by('time')
    serializer_class = MeteSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = MeteFilter
    # pagination_class = StandardResultsSetPagination


class LrViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = LrPegel.objects.all().order_by('time')
    serializer_class = LrSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = LrFilter


class ProjektViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Projekt.objects.all()
    serializer_class = ProjektSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = ProjektFilter


class AuswertungslaufSerializer(WritableNestedModelSerializer):
    lrpegel_set = LrSerializer(many=True)
    maxpegel_set = MaxPegelSerializer(many=True)
    
    detected_set = DetectedSerializer(many=True)
    rejected_set = RejectedSerializer(many=True)
    # schallleistungpegel_set = SchallleistungPegelSerializer(many=True)

    class Meta:
        model = Auswertungslauf
        fields = "__all__"
        # fields = ["detected_set", "rejected_set", "schallleistungpegel_set", "maxpegel_set", "lrpegel_set"] #"__all__"

class AuswertungslaufViewSet(viewsets.ModelViewSet):
    queryset = Auswertungslauf.objects.all()
    serializer_class = AuswertungslaufSerializer

    def create(self, request):
        logger.info("Start insertion")
        with transaction.atomic():
            result = super().create(request)
        logger.info("Insertion finished")
        return result


class ListLrPegel(APIView):
    """
    View to list lrPegel
    """

    def get(self, request, format=None):
        """
        Return lrPegel at io
        """
        try:
            time_after = request.GET['time_after']
            time_before = request.GET['time_before']
            immissionsort_id = request.GET['immissionsort']
        except MultiValueDictKeyError as e:
            print(e)
        usernames = my_custom_sql(time_after, time_before, immissionsort_id)
        return Response(usernames)


class ListMesspunktEvaluation(APIView):
    """
    View to list evakuation at mp
    """

    def get(self, request, format=None):
        """
        Return lrPegel at io
        """
        time_before = request.GET['time_before']
        time_after = request.GET['time_after']
        messpunkt_id = (request.GET['messpunkt'])
        projekt_id = request.GET.get("projekt", 1)
        usernames = get_auswertung_an_mp(time_before, time_after, messpunkt_id, projekt_id)
        return Response(usernames)

def get_monatsbericht(timepoint_in_month):
    pass

def get_auswertung_an_mp(time_before, time_after, messpunkt_id, projekt_id):
    with connection.cursor() as cursor:
        '2022-10-27 00:00:00'
        '2022-10-27 00:15:00'
        if True:
            q2 = "SELECT * FROM tsdb_detected d JOIN tsdb_messpunkt m on messpunkt_id = m.id WHERE m.projekt_id = 1 limit 10;"
            q1 = "SELECT * FROM tsdb_rejected r JOIN tsdb_messpunkt m on messpunkt_id = m.id WHERE m.projekt_id = 1 LIMIT 10"
            q_4 = f"""SELECT res.time as time, res.lafeq as lafeq, CASE WHEN d.time is NULL THEN NULL ELSE res.lafeq END as detected, CASE WHEN rej.time is NULL THEN NULL ELSE res.lafeq END as rejected, res.messpunkt_id as messpunkt_id, rej.ursache
            FROM (SELECT * FROM tsdb_resu r WHERE r.time >= '{time_after}' AND r.time <= '{time_before}' AND r.messpunkt_id = {messpunkt_id}) res
            LEFT JOIN 
            (SELECT * FROM tsdb_detected d JOIN tsdb_messpunkt m on messpunkt_id = m.id WHERE m.projekt_id = {projekt_id} and d.time >= '{time_after}' AND d.time <= '{time_before}') d ON res.time >= d.time AND res.time <= (d.time + (INTERVAL '1 sec' * d.dauer))
            LEFT JOIN 
            (SELECT time, u.name as ursache, m.name as messpunkt_name FROM tsdb_rejected r JOIN tsdb_messpunkt m ON messpunkt_id = m.id AND r.time >= '{time_after}' AND r.time <= '{time_before}'
            JOIN tsdb_rejection u ON r.filter_id = u.id WHERE m.projekt_id = {projekt_id}) rej 
            ON res.time = rej.time 
             ORDER by time;

            """
            q_tz = """SET TIME ZONE 'Europe/Rome'"""

            print(q_4)
            cursor.execute(q_tz)
            cursor.execute(q_4)
            return dictfetchall(cursor)

def my_custom_sql(time_after, time_before, immissionsort_id):
    with connection.cursor() as cursor:
        if True:
            q2 = "select * from tsdb_lrpegel l Join tsdb_laermursacheanimmissionsorten u on l.verursacht_id = u.id where l.time >= '2022-10-26' and l.time <= '2022-10-27';"
            q4 = """SET TIME ZONE 'Europe/Rome'"""
            q3 = f"select * FROM (select verursacht_id, json_agg(json_build_object('time', time + interval '0 hour', 'pegel', pegel) Order by time) AS ts from tsdb_lrpegel lr where time >= '{time_after}' and time <= '{time_before}' and immissionsort_id = {immissionsort_id} group by verursacht_id) lr JOIN tsdb_laermursacheanimmissionsorten u ON u.id = lr.verursacht_id;"
            print(q3)
            cursor.execute(q4)
            cursor.execute(q3)
            result_dict =  dictfetchall(cursor)
            print(result_dict)
            return result_dict



def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]

    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

