#!/usr/bin/env python3
"""Test cases for the Weekly Disagreement Scanner detection logic."""

from weekly_disagreement_scanner import detect_signals

def test_1_avgo_5week_bullish():
    weeks = [
        {"date":"2026-02-02","open":330,"high":350,"low":312,"close":340,"volume":1e6},
        {"date":"2026-02-09","open":340,"high":360,"low":325,"close":335,"volume":1e6},
        {"date":"2026-02-16","open":335,"high":345,"low":318,"close":330,"volume":1e6},
        {"date":"2026-02-23","open":330,"high":342,"low":320,"close":335,"volume":1e6},
        {"date":"2026-03-02","open":335,"high":355,"low":322,"close":340,"volume":1e6},
        {"date":"2026-03-09","open":340,"high":350,"low":305,"close":345,"volume":2e6},
    ]
    sigs = detect_signals("AVGO", weeks)
    bull = [s for s in sigs if s["direction"] == "BULLISH" and s["date"] == "2026-03-09"]
    assert len(bull) == 1, f"Expected 1 bullish signal, got {len(bull)}"
    s = bull[0]
    assert s["weeks_swept"] == 5, f"Expected 5 weeks swept, got {s['weeks_swept']}"
    assert s["grade"] == "A", f"Expected grade A, got {s['grade']}"
    assert s["score"] >= 80, f"Expected score ~85, got {s['score']}"
    print(f"  PASS: AVGO 5-week bullish | swept={s['weeks_swept']} grade={s['grade']} score={s['score']}")

def test_2_be_2week_bullish():
    weeks = [
        {"date":"2026-01-05","open":80,"high":95,"low":75,"close":90,"volume":1e6},
        {"date":"2026-01-12","open":90,"high":110,"low":85,"close":105,"volume":1e6},
        {"date":"2026-01-19","open":105,"high":130,"low":100,"close":125,"volume":1e6},
        {"date":"2026-01-26","open":125,"high":140,"low":118,"close":135,"volume":1e6},
        {"date":"2026-02-02","open":135,"high":145,"low":125,"close":140,"volume":1e6},
        {"date":"2026-02-09","open":130,"high":142,"low":115,"close":138,"volume":2e6},
    ]
    sigs = detect_signals("BE", weeks)
    bull = [s for s in sigs if s["direction"] == "BULLISH" and s["date"] == "2026-02-09"]
    assert len(bull) == 1, f"Expected 1 bullish signal, got {len(bull)}"
    s = bull[0]
    assert s["weeks_swept"] == 2, f"Expected 2 weeks swept, got {s['weeks_swept']}"
    assert s["grade"] == "A", f"Expected grade A, got {s['grade']}"
    print(f"  PASS: BE 2-week bullish | swept={s['weeks_swept']} grade={s['grade']} score={s['score']}")

def test_3_bearish_4week():
    weeks = [
        {"date":"2026-01-05","open":50,"high":55,"low":45,"close":48,"volume":1e6},
        {"date":"2026-01-12","open":48,"high":52,"low":44,"close":50,"volume":1e6},
        {"date":"2026-01-19","open":50,"high":54,"low":46,"close":52,"volume":1e6},
        {"date":"2026-01-26","open":52,"high":53,"low":48,"close":51,"volume":1e6},
        {"date":"2026-02-02","open":53,"high":56,"low":47,"close":50,"volume":2e6},
    ]
    sigs = detect_signals("TEST", weeks)
    bear = [s for s in sigs if s["direction"] == "BEARISH" and s["date"] == "2026-02-02"]
    assert len(bear) == 1, f"Expected 1 bearish signal, got {len(bear)}"
    s = bear[0]
    assert s["weeks_swept"] == 4, f"Expected 4 weeks swept, got {s['weeks_swept']}"
    assert s["grade"] == "A", f"Expected grade A, got {s['grade']}"
    print(f"  PASS: Bearish 4-week | swept={s['weeks_swept']} grade={s['grade']} score={s['score']}")

def test_4_only_1week_no_signal():
    weeks = [
        {"date":"2026-01-05","open":100,"high":110,"low":90,"close":105,"volume":1e6},
        {"date":"2026-01-12","open":105,"high":125,"low":98,"close":108,"volume":1e6},
        {"date":"2026-01-19","open":108,"high":120,"low":102,"close":110,"volume":1e6},
        # low=101 only below wk3 low (102), NOT below wk2 low (98) → only 1 week swept
        {"date":"2026-01-26","open":108,"high":115,"low":101,"close":112,"volume":1e6},
    ]
    sigs = detect_signals("TEST", weeks)
    assert len(sigs) == 0, f"Expected 0 signals, got {len(sigs)}: {[s['direction'] + ' ' + s['date'] for s in sigs]}"
    print(f"  PASS: 1-week sweep correctly rejected (0 signals)")

def test_5_close_outside_pw_no_signal():
    weeks = [
        {"date":"2026-01-05","open":100,"high":110,"low":90,"close":105,"volume":1e6},
        {"date":"2026-01-12","open":105,"high":115,"low":95,"close":108,"volume":1e6},
        {"date":"2026-01-19","open":108,"high":112,"low":97,"close":110,"volume":1e6},
        {"date":"2026-01-26","open":100,"high":105,"low":88,"close":93,"volume":1e6},
    ]
    sigs = detect_signals("TEST", weeks)
    assert len(sigs) == 0, f"Expected 0 signals, got {len(sigs)}: {[s['direction'] + ' ' + s['date'] for s in sigs]}"
    print(f"  PASS: Close outside PW range correctly rejected (0 signals)")

if __name__ == "__main__":
    print("Running disagreement scanner tests...\n")
    test_1_avgo_5week_bullish()
    test_2_be_2week_bullish()
    test_3_bearish_4week()
    test_4_only_1week_no_signal()
    test_5_close_outside_pw_no_signal()
    print("\nAll 5 tests passed!")
