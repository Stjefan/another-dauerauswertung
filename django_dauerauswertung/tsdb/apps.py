from django.apps import AppConfig


class TsdbConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'tsdb'
    def ready(self):
        import tsdb.signals
