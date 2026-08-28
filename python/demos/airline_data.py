# Airline on-time performance data, shaped like the real thing.
#
# The reference tables are real: IATA carrier and airport codes, with the cities and
# states they belong to. The flights are generated, because the actual BTS on-time
# performance files are hundreds of megabytes a month and cannot be committed — but they
# are generated to behave like the real ones, which is what matters for debugging:
#
#   * delays are heavy-tailed, not uniform. Most flights are near on time, a few are
#     hours late, and the tail is where every interesting bug lives.
#   * carriers differ from each other, and hub airports differ from spokes, so a
#     grouped aggregate has something to say.
#   * some flights are cancelled and carry no arrival time at all, so the null handling
#     in a pipeline is exercised rather than assumed away.
#   * a small fraction of rows are malformed the way real feeds are: a delay recorded
#     as text, a missing airport code.
#
# Everything is deterministic in the seed, so a notebook that runs twice says the same
# thing twice.

import random
import zlib

# --- reference data (real codes) -------------------------------------------

CARRIERS = [
    ("AA", "American Airlines"), ("DL", "Delta Air Lines"),
    ("UA", "United Air Lines"), ("WN", "Southwest Airlines"),
    ("AS", "Alaska Airlines"), ("B6", "JetBlue Airways"),
    ("NK", "Spirit Air Lines"), ("F9", "Frontier Airlines"),
    ("HA", "Hawaiian Airlines"), ("G4", "Allegiant Air"),
]

# code, name, city, state, and whether it is a major hub
AIRPORTS = [
    ("ATL", "Hartsfield-Jackson Atlanta International", "Atlanta", "GA", True),
    ("DFW", "Dallas/Fort Worth International", "Dallas-Fort Worth", "TX", True),
    ("DEN", "Denver International", "Denver", "CO", True),
    ("ORD", "Chicago O'Hare International", "Chicago", "IL", True),
    ("LAX", "Los Angeles International", "Los Angeles", "CA", True),
    ("CLT", "Charlotte Douglas International", "Charlotte", "NC", True),
    ("LAS", "Harry Reid International", "Las Vegas", "NV", True),
    ("PHX", "Phoenix Sky Harbor International", "Phoenix", "AZ", True),
    ("MCO", "Orlando International", "Orlando", "FL", True),
    ("SEA", "Seattle-Tacoma International", "Seattle", "WA", True),
    ("MIA", "Miami International", "Miami", "FL", False),
    ("IAH", "George Bush Intercontinental", "Houston", "TX", False),
    ("JFK", "John F. Kennedy International", "New York", "NY", False),
    ("EWR", "Newark Liberty International", "Newark", "NJ", False),
    ("SFO", "San Francisco International", "San Francisco", "CA", False),
    ("BOS", "Boston Logan International", "Boston", "MA", False),
    ("DTW", "Detroit Metropolitan Wayne County", "Detroit", "MI", False),
    ("MSP", "Minneapolis-St Paul International", "Minneapolis", "MN", False),
    ("PHL", "Philadelphia International", "Philadelphia", "PA", False),
    ("SLC", "Salt Lake City International", "Salt Lake City", "UT", False),
    ("BWI", "Baltimore/Washington International", "Baltimore", "MD", False),
    ("SAN", "San Diego International", "San Diego", "CA", False),
    ("TPA", "Tampa International", "Tampa", "FL", False),
    ("PDX", "Portland International", "Portland", "OR", False),
    ("BNA", "Nashville International", "Nashville", "TN", False),
    ("AUS", "Austin-Bergstrom International", "Austin", "TX", False),
    ("STL", "St. Louis Lambert International", "St. Louis", "MO", False),
    ("RDU", "Raleigh-Durham International", "Raleigh-Durham", "NC", False),
    ("MCI", "Kansas City International", "Kansas City", "MO", False),
    ("SMF", "Sacramento International", "Sacramento", "CA", False),
]

FLIGHTS_SCHEMA = (
    "flight_id STRING, day STRING, carrier STRING, origin STRING, dest STRING, "
    "sched_dep INT, dep_delay INT, arr_delay INT, distance INT, cancelled INT"
)
AIRPORTS_SCHEMA = "code STRING, name STRING, city STRING, state STRING, hub BOOLEAN"
CARRIERS_SCHEMA = "code STRING, name STRING"

# Carriers are not equally punctual, and the differences are what a grouped aggregate is
# for. These shift the centre of each carrier's delay distribution, in minutes.
_PUNCTUALITY = {
    "AA": 4, "DL": -1, "UA": 6, "WN": 2, "AS": -3,
    "B6": 9, "NK": 12, "F9": 10, "HA": -5, "G4": 8,
}


def airports():
    """The airport reference table."""
    return [(code, name, city, state, hub) for code, name, city, state, hub in AIRPORTS]


def carriers():
    """The carrier reference table."""
    return list(CARRIERS)


def flights(count, seed=0, malformed_rate=0.0005):
    """``count`` generated flights.

    ``malformed_rate`` injects the kind of damage a real feed carries — a missing
    airport code, an implausible delay. Set it to 0 for clean data.
    """
    random = _Random(seed)
    codes = [a[0] for a in AIRPORTS]
    hubs = [a[0] for a in AIRPORTS if a[4]]
    rows = []

    for i in range(count):
        carrier = random.choice([c[0] for c in CARRIERS])

        # traffic concentrates on hubs, as it does in the real network
        origin = random.choice(hubs) if random.random() < 0.55 else random.choice(codes)
        dest = random.choice(hubs) if random.random() < 0.45 else random.choice(codes)
        while dest == origin:
            dest = random.choice(codes)

        day = "2026-%02d-%02d" % (1 + i % 12, 1 + (i // 12) % 28)
        sched_dep = random.choice([600, 725, 810, 905, 1030, 1145, 1320, 1450,
                                   1605, 1730, 1845, 2010, 2130])
        distance = _route_distance(origin, dest)

        cancelled = 1 if random.random() < 0.018 else 0

        if cancelled:
            dep_delay, arr_delay = None, None
        else:
            # Heavy tail: most flights are close to on time, a few are badly late.
            # A normal distribution would hide exactly the records worth debugging.
            centre = _PUNCTUALITY[carrier]
            if random.random() < 0.80:
                dep_delay = int(random.gauss(centre - 4, 12))
            elif random.random() < 0.85:
                dep_delay = int(random.gauss(centre + 45, 30))
            else:
                dep_delay = int(random.gauss(centre + 240, 120))   # the tail
            arr_delay = dep_delay + int(random.gauss(-3, 9))

        if malformed_rate and random.random() < malformed_rate:
            # the shapes a real feed actually arrives in
            damage = random.choice(["no_origin", "absurd_delay"])
            if damage == "no_origin":
                origin = None
            else:
                arr_delay = 100000

        rows.append((("F%07d" % i), day, carrier, origin, dest,
                     sched_dep, dep_delay, arr_delay, distance, cancelled))
    return rows


def _route_distance(origin, dest):
    """A stand-in great-circle distance for a route, in miles.

    `zlib.crc32` rather than the built-in `hash`, which is salted per process: with
    `hash` the same route came out a different length on every run, so `haul` — and
    therefore every group the analysis produces — was not reproducible. That is a poor
    property for data a debugging demo is supposed to be examined against.
    """
    return 200 + (zlib.crc32((origin + dest).encode()) % 2400)


class _Random(random.Random):
    """A seeded generator, so the same scale always produces the same flights."""

    def __init__(self, seed):
        super(_Random, self).__init__()
        self.seed(seed)
