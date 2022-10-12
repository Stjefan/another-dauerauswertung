from django.shortcuts import render

from rest_framework import viewsets
from rest_framework import serializers
from .models import EvaluationMesspunkt, Immissionsort, LrPegel, Messpunkt, Projekt, Resu
from django.urls import path, include
from django_filters import rest_framework as filters
from django_filters import FilterSet, DateTimeFromToRangeFilter

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
        fields = ["name"]

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
    
class ResuSerializer(serializers.ModelSerializer):
    class Meta:
        model = Resu
        fields = ['time', 'lafeq', 'lafmax']

class ResuFilter(FilterSet):
    time = DateTimeFromToRangeFilter()
    class Meta:
        model = Resu
        fields = {
            'time': [],
            'messpunkt': ['exact']
        }
class ResuViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Resu.objects.all()
    serializer_class = ResuSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = ResuFilter


class LrViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = LrPegel.objects.all()
    serializer_class = LrSerializer
    filter_backends = (filters.DjangoFilterBackend,)
    filterset_class = LrFilter


class ProjektViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Projekt.objects.all()
    serializer_class = ProjektSerializer
