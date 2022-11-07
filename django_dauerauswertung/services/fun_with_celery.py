# py -m celery -A fun_with_celery worker --loglevel=INFO -P solo

from time import sleep
from celery import Celery

app = Celery('hello', broker='amqp://guest@localhost//', backend="db+postgresql://postgres:password@localhost/tsdb")
#'db+sqlite:///results.sqlite')

@app.task
def hello():
    sleep(1)
    print("hello")
    sleep(2)
    print("Another")
    sleep(3)
    print("End")
    return 'hello world'


@app.task
def add(x, y):
    sleep(1)
    print("adding")
    sleep(2)
    print("waitng")
    return x+y
