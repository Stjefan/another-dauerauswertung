from django.shortcuts import render

from rest_framework import viewsets
from rest_framework import serializers
from .models import EvaluationMesspunkt, Immissionsort, LrPegel, Messpunkt, Projekt, Resu, Terz, Mete
from django.urls import path, include
from django_filters import rest_framework as filters
from django_filters import FilterSet, DateTimeFromToRangeFilter
from rest_framework.pagination import PageNumberPagination


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 1000

# Create your views here.
class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = EvaluationMesspunkt
        fields = ['time', 'lafeq', 'rejected', 'detected']


class LrSerializer(serializers.ModelSerializer):
    class Meta:
        model = LrPegel
        fields = ['time', 'pegel', 'verursacht', 'immissionsort']

class MessspunktSerializer(serializers.ModelSerializer):
    class Meta:
        model = Messpunkt
        fields = ["name", "schallursache_set"]

class ImmissionsortSerializer(serializers.ModelSerializer):
    class Meta:
        model = Immissionsort
        fields = ["name", "grenzwert_tag", "grenzwert_nacht"]

class ProjektSerializer(serializers.ModelSerializer):
    messpunkt_set = MessspunktSerializer(many=True)
    immissionsort_set = ImmissionsortSerializer(many=True)
    class Meta:
        model = Projekt
        fields = ["name", "id", "messpunkt_set", "immissionsort_set"]

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
