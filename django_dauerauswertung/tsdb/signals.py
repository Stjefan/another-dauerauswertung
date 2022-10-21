from django.db.models.signals import post_save, post_delete
from django.contrib.auth.models import User
from django.dispatch import receiver
from .models import Auswertungslauf, LrPegel
from .helpers import get_beurteilungszeitraum_start
import logging

@receiver(post_save, sender=Auswertungslauf)
def create_profile(sender, instance, created, **kwargs):
    if created:
        logging.info("Start deletion")
        lb_beurteilungszeitraum, ub_beurteilugnszeitraum = get_beurteilungszeitraum_start(instance.zeitpunkt_im_beurteilungszeitraum)
        others = Auswertungslauf.objects.filter(zeitpunkt_im_beurteilungszeitraum__gte=lb_beurteilungszeitraum, zeitpunkt_im_beurteilungszeitraum__lte=ub_beurteilugnszeitraum).exclude(id=instance.id)
        for i in others:
            i.delete()
        logging.info("Deletion finished")
        # Auswertungslauf.objects.create(user=instance)
        
@receiver(post_delete, sender=LrPegel)
def delete_cb(sender, instance, **kwargs):
    logging.info("Deleted lrPegel")  

