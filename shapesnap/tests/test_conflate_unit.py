"""shapesnap.conflate unit tests — normalization, fuzzy match, precision.

Real-data conflation (O'Hare move, feed 5 bus-name wins) lives in the
re-match receipts; these are the fast unit-level guards on the matching
logic and the precision policy (prefer misses over wrong matches).
"""

from __future__ import annotations

from shapesnap.candidates import Station
from shapesnap.conflate import (
    ConflateConfig,
    conflate_mode,
    name_similarity_lenient,
    normalize_name,
    trigram_similarity,
)


def test_normalize_expansions():
    assert normalize_name("BWAY/W 42 ST") == "bway west 42 street"
    assert normalize_name("Grand Concourse Blvd") == "grand concourse boulevard"
    assert normalize_name("St. George") == "saint george"
    assert normalize_name("5th Av & 42 St") == "5th avenue and 42 street"
    assert normalize_name("Times Sq (42 St)") == "times square"
    assert normalize_name("Ft Hamilton Pkwy") == "fort hamilton parkway"


def test_name_similarity_lenient_accepts_real_variants():
    # abbreviation drift the token sets differ on, trigram rescues
    assert name_similarity_lenient("104 St", "104th Street") >= 0.6
    assert name_similarity_lenient("W 8 St-NY Aquarium",
                                   "West 8th Street–New York Aquarium") >= 0.6
    # superset token match
    assert name_similarity_lenient("Damen-O'Hare", "Damen") == 1.0
    assert name_similarity_lenient("O'Hare", "O'Hare") == 1.0


def test_name_similarity_rejects_unrelated():
    assert name_similarity_lenient("Pulaski", "Cicero") < 0.6
    assert name_similarity_lenient("Times Sq", "Grand Central") < 0.6
    assert name_similarity_lenient("", "Anything") == 0.0


def test_trigram_symmetry_and_bounds():
    assert trigram_similarity("abc", "abc") == 1.0
    assert 0.0 <= trigram_similarity("abcd", "abce") <= 1.0
    assert trigram_similarity("", "x") == 0.0


def _st(lon, lat, name, ref=""):
    return Station(lon, lat, name, ref)


def test_conflate_moves_named_match_within_radius():
    # a GTFS stop 90 m from a name-matching OSM stop moves onto it
    cfg = ConflateConfig()
    # ~90 m east at this latitude
    gtfs = [("A", -87.9042, 41.9777, "O'Hare")]
    osm = [_st(-87.9031, 41.9780, "O'Hare")]
    res = conflate_mode(gtfs, osm, "rail", cfg)
    assert res.matched == 1
    ov = res.overrides["A"]
    assert ov[2] == "O'Hare"       # new name
    assert ov[0] == round(-87.9031, 7)  # moved to OSM lon
    assert 60 < ov[5] < 150        # dist moved


def test_conflate_prefers_miss_over_wrong_type_name():
    # a nearby OSM stop with an UNRELATED name is never matched — the stop
    # keeps its GTFS position (prefer misses over wrong matches)
    cfg = ConflateConfig()
    gtfs = [("A", -87.9042, 41.9777, "O'Hare")]
    osm = [_st(-87.9041, 41.9778, "Rosemont")]  # 10 m away, wrong name
    res = conflate_mode(gtfs, osm, "rail", cfg)
    assert res.matched == 0
    assert "A" not in res.overrides


def test_conflate_respects_radius():
    cfg = ConflateConfig(radius_m={"rail": 50.0})
    gtfs = [("A", -87.9042, 41.9777, "O'Hare")]
    osm = [_st(-87.9031, 41.9780, "O'Hare")]  # ~90 m > 50 m radius
    res = conflate_mode(gtfs, osm, "rail", cfg)
    assert res.matched == 0


def test_conflate_picks_best_named_candidate_when_several_qualify():
    # two name-similar OSM stops in radius: the closer one wins (proximity
    # tiebreak) when name similarity ties
    cfg = ConflateConfig()
    gtfs = [("A", -87.9042, 41.9777, "O'Hare")]
    osm = [
        _st(-87.9020, 41.9782, "O'Hare"),   # farther
        _st(-87.9033, 41.9779, "O'Hare"),   # closer
    ]
    res = conflate_mode(gtfs, osm, "rail", cfg)
    assert res.matched == 1
    assert res.overrides["A"][0] == round(-87.9033, 7)
