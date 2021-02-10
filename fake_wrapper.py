from faker import Faker
from faker.providers import isbn
from datetime import datetime

def current_date(self):
    return datetime.now().isoformat()

def current_block_from(self):
    block = datetime.now()
    block = block.replace(second=0, microsecond=0)
    return block

def building_name(self):
    return self.fake.company()

def temperatura(self):
    return self.fake.pyfloat(min_value=-10, max_value=2, right_digits=2)

def energy(self):
    return self.fake.pyfloat(min_value=205, max_value=230, right_digits=2)

def humidity(self):
    return self.fake.pyfloat(min_value=0, max_value=100, right_digits=2)

def location(self):
    coord = self.fake.local_latlng(country_code='ES', coords_only=True)
    return [ float(coord[1]), float(coord[0]) ]
    #return [ float(coord[0]), float(coord[1]) ]

def cacharro(fake, cacharro):
    return {
        "cacharro": "Maquina %d" % cacharro
    }

def current_cacharro(self):
    return self.cacharro

extra_providers = {
    "current_date": current_date,
    "building": building_name,
    "current_block_from": current_block_from,
    "temperatura": temperatura,
    "energy": energy,
    "humidity": humidity,
    "location": location,
    "cacharro": current_cacharro
}

class FakeWrapper:
    def __init__(self, num_cacharros=500):
        self._cacharro = None
        self.fake = Faker()
        self.fake.add_provider(isbn)
        self._cacharros = {}
        for i in range(0, num_cacharros):
            self.add_cacharro(i)

    def add_cacharro(self, id):
        self._cacharros[id] = cacharro(self, id)

    def current_cacharro(self, id):
        self.cacharro = self._cacharros[id]

    def __getitem__(self, key):
        if key in extra_providers:
            return extra_providers[key](self)

        return fake.__getattr__(key)()
