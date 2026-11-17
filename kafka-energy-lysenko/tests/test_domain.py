import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from common import analyze_power_data, generate_power_data


class TelemetryRulesTest(unittest.TestCase):
    def record(self, **changes):
        record = generate_power_data(random.Random(61), 1, "test")
        record.update(power_output_mw=900, voltage_kv=220, frequency_hz=50, efficiency_percent=85)
        record.update(changes)
        return record

    def test_warning_thresholds_include_safe_boundaries(self):
        for voltage in (218, 222):
            self.assertEqual(analyze_power_data(self.record(voltage_kv=voltage))["warnings"], [])
        for frequency in (49.8, 50.2):
            self.assertEqual(analyze_power_data(self.record(frequency_hz=frequency))["warnings"], [])
        for frequency in (49.79, 50.21):
            self.assertTrue(analyze_power_data(self.record(frequency_hz=frequency))["warnings"])

    def test_critical_values_raise_three_warnings(self):
        result = analyze_power_data(self.record(voltage_kv=223.5, frequency_hz=50.3, efficiency_percent=72))
        self.assertEqual(len(result["warnings"]), 3)

    def test_bad_telemetry_cannot_silently_become_zero(self):
        for value in (float("nan"), float("inf"), True, "220", None):
            with self.assertRaises(ValueError):
                analyze_power_data(self.record(voltage_kv=value))
        for record in ({}, [], self.record(efficiency_percent=101), self.record(power_output_mw=-1)):
            with self.assertRaises(ValueError):
                analyze_power_data(record)

    def test_generated_values_stay_within_station_capacity(self):
        maxima = {"thermal": 1200, "hydro": 800, "solar": 150, "wind": 200}
        rng = random.Random(61)
        for sequence in range(1, 201):
            record = generate_power_data(rng, sequence, "test")
            self.assertLessEqual(record["power_output_mw"], maxima[record["station_type"]])
            self.assertGreaterEqual(record["power_output_mw"], 0)
            analyze_power_data(record)


if __name__ == "__main__":
    unittest.main()
