from django.contrib import admin

from tsdb.models import Projekt, LaermursacheAnImmissionsorten,Rejection, Detection, Auswertungslauf, Immissionsort, Messpunkt

# Register your models here.

admin.site.register(Projekt)
admin.site.register(LaermursacheAnImmissionsorten)
admin.site.register(Rejection)
admin.site.register(Detection)
admin.site.register(Auswertungslauf)
admin.site.register(Messpunkt)
admin.site.register(Immissionsort)