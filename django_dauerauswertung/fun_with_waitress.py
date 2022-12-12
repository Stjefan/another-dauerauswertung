from django_dauerauswertung.wsgi import application as wsgiapp


import logging
logger = logging.getLogger('waitress')
logger.setLevel(logging.DEBUG)

from waitress import serve
serve(wsgiapp, host='0.0.0.0', port=8002, url_prefix='/reverse/')
