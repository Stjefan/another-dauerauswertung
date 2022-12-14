from django.shortcuts import render

from rest_framework import viewsets
from rest_framework import serializers
from .models import (Ausbreitstungsfaktor, EvaluationMesspunkt, Immissionsort, Messpunkt,
LrPegel, SchallleistungPegel, MaxPegel, Detected, Rejected, 
Projekt, Resu, Terz, Mete, LaermursacheAnImmissionsorten, LaermursacheAnMesspunkt, Auswertungslauf)
from django.urls import path, include
from django_filters import rest_framework as filters
from django_filters import FilterSet, DateTimeFromToRangeFilter
from rest_framework.pagination import PageNumberPagination
from drf_writable_nested.serializers import WritableNestedModelSerializer
from django.db import transaction
import logging

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
        fields = ["name", "gk_rechts", "gk_hoch", "id", "is_meteo_station", "laermursacheanmesspunkt_set", "id_external", "lwa"]

class ImmissionsortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Immissionsort
        fields = ["name", "grenzwert_tag", "grenzwert_nacht", "gk_rechts", "gk_hoch", "id_external", "id"]

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

class ProjektSerializer(serializers.ModelSerializer):
    messpunkt_set = MessspunktSerializer(many=True)
    immissionsort_set = ImmissionsortSerializer(many=True)
    laermursacheanimmissionsorten_set = LaermursacheAnImmissionsortenSerializer(many=True)

    ausbreitungsfaktoren_set = serializers.SerializerMethodField()

    def get_ausbreitungsfaktoren_set(self, obj):
        return [
        AusbreitungsfaktorSerializer(abf).data 
        for mp in obj.messpunkt_set.all() for abf in mp.ausbreitstungsfaktor_set.all()]


    class Meta:
        model = Projekt
        fields = ["name", "id", "messpunkt_set", "immissionsort_set", "laermursacheanimmissionsorten_set", "ausbreitungsfaktoren_set"]

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
        fields = ['time']

class ResuFilter(FilterSet):
    time = DateTimeFromToRangeFilter()
    class Meta:
        model = Resu
        fields = {
            'time': [],
            'messpunkt': ['exact']
        }
class ResuViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Resu.objects.all().order_by('-time')
    serializer_class = ResuSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = ResuFilter
    pagination_class = StandardResultsSetPagination


class TerzViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Terz.objects.all().order_by('-time')
    serializer_class = TerzSerializer
    # filter_backends = (filters.DjangoFilterBackend,)
    # filterset_class = ResuFilter
    pagination_class = StandardResultsSetPagination

class MeteViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Mete.objects.all().order_by('-time')
    serializer_class = MeteSerializer
    # filter_backends = (filters.DjangoFilterBackend,)
    # filterset_class = ResuFilter
    pagination_class = StandardResultsSetPagination


class LrViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = LrPegel.objects.all()
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

