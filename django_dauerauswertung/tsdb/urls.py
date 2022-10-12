from django.conf import settings
from django.urls import path, include

from django.conf.urls.static import static


from . import views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'evaluation', views.UserViewSet,basename="evaluation-messpunkt")
router.register(r'resu', views.ResuViewSet,basename="resu")



urlpatterns = [
    path('', include(router.urls)),
]