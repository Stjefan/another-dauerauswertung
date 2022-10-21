import jsonpickle

class Projekt(object):
    def __init__(self, name):
        self.name = name

p1 = Projekt('Awesome')


frozen = jsonpickle.encode(p1)

print(frozen)
p2 = jsonpickle.decode(frozen)