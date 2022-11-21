from dataclasses import field, dataclass
from datetime import datetime, timedelta
from typing import Tuple, Dict
from tsdb.models import Immissionsort, Projekt

def get_beurteilungszeitraum_start(arg: datetime):
    if 6<= arg.hour <= 21:
        return datetime(arg.year, arg.month, arg.day, 6, 0, 0), datetime(arg.year, arg.month, arg.day, 21, 59, 59)
    else:
        return datetime(arg.year, arg.month, arg.day, arg.hour, 0, 0), datetime(arg.year, arg.month, arg.day, arg.hour, 0, 0) + timedelta(hours=1, seconds=-1)

@dataclass
class MonatsuebersichtAnImmissionsortV2:
    immissionsort: Immissionsort = None
    lr_tag: Dict[int, float] = field(default_factory=dict)
    lr_max_nacht: Dict[int, Tuple[float, int]] = field(default_factory=dict)
    lauteste_stunde_tag:  Dict[int, float] = field(default_factory=dict)
    lauteste_stunde_nacht: Dict[int, Tuple[float, int]] = field(default_factory=dict)


@dataclass
class Monatsbericht:
    monat: datetime
    projekt: Projekt
    no_verwertbare_sekunden: int
    no_aussortiert_wetter: int
    no_aussortiert_sonstige: int
    ueberschrift: str
    details_io: Dict[int, MonatsuebersichtAnImmissionsortV2]
    schallleistungspegel: Dict[Tuple[int, int], float] = None
  