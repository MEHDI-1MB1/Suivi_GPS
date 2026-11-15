"""Masque terre/mer pour la côte atlantique du Maroc (approximatif, à ~2-3 km près)."""
import math
import random

# (latitude, longitude de la côte), du sud au nord : à l'est de la côte = terre ferme
COAST = [(29.00, -10.20), (30.00, -9.80), (30.42, -9.64), (30.55, -9.72), (31.00, -9.80), (31.51, -9.79),
         (31.95, -9.45), (32.29, -9.25), (32.55, -9.27), (32.73, -9.05), (33.12, -8.65), (33.23, -8.53),
         (33.27, -8.50), (33.29, -8.36), (33.52, -7.83), (33.59, -7.68), (33.61, -7.60), (33.71, -7.38),
         (33.79, -7.16), (33.85, -7.03), (33.92, -6.93), (34.03, -6.85), (34.08, -6.80), (34.26, -6.67),
         (34.88, -6.30), (35.20, -6.16), (35.47, -6.03), (35.78, -5.92)]
MARGIN = 0.04          # ~3,5 km à l'intérieur des terres


def coast_lon(lat):
    if lat <= COAST[0][0]:
        return COAST[0][1]
    for (a, x), (b, y) in zip(COAST, COAST[1:]):
        if a <= lat <= b:
            return x + (y - x) * (lat - a) / (b - a)
    return None


def is_land(lat, lon, margin=0.0):
    if lat > 35.78:                      # détroit de Gibraltar : on reste sur la terre ferme
        return lat < 35.78 and lon > -5.88
    return lon > coast_lon(lat) + margin


def to_land(lat, lon, margin=MARGIN):
    """Ramène un point en mer sur la terre ferme (fonction continue : un trajet reste continu)."""
    if lat > 35.78:
        return min(lat, 35.77), max(lon, -5.88)
    c = coast_lon(lat)
    return (lat, c + margin) if lon < c + margin else (lat, lon)


def random_land_point(rng, lat0, lon0, radius_km, tries=30):
    """Point aléatoire dans un disque, tiré jusqu'à tomber sur terre (pas de points collés à la côte)."""
    for _ in range(tries):
        r = radius_km * math.sqrt(rng.random())
        b = rng.uniform(0, 2 * math.pi)
        lat = lat0 + r * math.cos(b) / 111.0
        lon = lon0 + r * math.sin(b) / (111.0 * math.cos(math.radians(lat0)))
        if is_land(lat, lon, MARGIN):
            return lat, lon
    return to_land(lat0, lon0)


def lerp_land(lat1, lon1, lat2, lon2, f):
    """Point à la fraction f du trajet origine -> destination, ramené sur terre :
    un trajet qui couperait la mer longe la côte."""
    return to_land(lat1 + (lat2 - lat1) * f, lon1 + (lon2 - lon1) * f)


def path_km(lat1, lon1, lat2, lon2, n=40):
    """Longueur du trajet (polyligne échantillonnée) en km."""
    R = 6371.0088
    pts = [lerp_land(lat1, lon1, lat2, lon2, i / n) for i in range(n + 1)]
    tot = 0.0
    for (a, b), (c, d) in zip(pts, pts[1:]):
        p1, p2 = math.radians(a), math.radians(c)
        x = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(d - b) / 2) ** 2
        tot += 2 * R * math.asin(math.sqrt(x))
    return tot


# ----------------------------------------------------------------------------
# Redéfinitions (remplacent lerp_land et path_km définies plus haut) :
# le point à la fraction f est placé selon l'ABSCISSE CURVILIGNE du chemin ramené sur terre.
# Avant : interpolation en ligne droite puis projection ; sur une côte oblique, la projection
# faisait glisser le point plus vite ou plus lentement que le véhicule (jusqu'à 2 km par pas de 30 s).
# ----------------------------------------------------------------------------
import math as _math
from bisect import bisect_right as _bisect_right
from functools import lru_cache as _lru_cache

_N_PATH = 120   # points d'échantillonnage du chemin projeté


def _hav_km(lat1, lon1, lat2, lon2):
    p1, p2 = _math.radians(lat1), _math.radians(lat2)
    a = (_math.sin((p2 - p1) / 2) ** 2
         + _math.cos(p1) * _math.cos(p2) * _math.sin(_math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371.0088 * _math.asin(_math.sqrt(a))


@_lru_cache(maxsize=50000)
def _route(lat1, lon1, lat2, lon2):
    pts = [to_land(lat1 + (lat2 - lat1) * k / _N_PATH, lon1 + (lon2 - lon1) * k / _N_PATH)
           for k in range(_N_PATH + 1)]
    cum = [0.0]
    for (a, b), (c, d) in zip(pts, pts[1:]):
        cum.append(cum[-1] + _hav_km(a, b, c, d))
    return tuple(pts), tuple(cum)


def lerp_land(lat1, lon1, lat2, lon2, f):
    pts, cum = _route(lat1, lon1, lat2, lon2)
    total = cum[-1]
    if total <= 0:
        return pts[0]
    target = min(max(f, 0.0), 1.0) * total
    i = min(_bisect_right(cum, target) - 1, len(pts) - 2)
    seg = cum[i + 1] - cum[i]
    t = 0.0 if seg <= 0 else (target - cum[i]) / seg
    return (pts[i][0] + (pts[i + 1][0] - pts[i][0]) * t,
            pts[i][1] + (pts[i + 1][1] - pts[i][1]) * t)


def path_km(lat1, lon1, lat2, lon2, n=40):
    """Longueur (km) du chemin ramené sur terre ; cohérente avec lerp_land."""
    return _route(lat1, lon1, lat2, lon2)[1][-1]
