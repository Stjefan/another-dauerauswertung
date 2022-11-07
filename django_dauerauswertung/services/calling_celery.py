from fun_with_celery import hello

print(hello.delay())

task = hello.delay()

print(task.get())