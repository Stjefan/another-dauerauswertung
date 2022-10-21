from django.conf import settings
from django.urls import path, include

from django.conf.urls.static import static


from . import views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'evaluation', views.UserViewSet,basename="evaluation-messpunkt")
router.register(r'resu', views.ResuViewSet,basename="resu")
router.register(r'terz', views.TerzViewSet,basename="terz")
router.register(r'mete', views.MeteViewSet,basename="mete")
router.register(r'lr', views.LrViewSet,basename="lr")
router.register(r'auswertungslauf', views.AuswertungslaufViewSet,basename="auswertungslauf")
router.register(r'projekt', views.ProjektViewSet,basename="projekt")



urlpatterns = [
    path('', include(router.urls)),
]