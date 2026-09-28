"""配送の欠落・破損を性能値として取り込まないことを検証する。"""
import json
from pathlib import Path
import tempfile
import unittest

from reliable_metrics import expected_fingerprint, measure, report


class ReceiptVerificationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.workload = dict(name="fixture", short_rate=2, bulk_rate=0, short_bytes=1, bulk_bytes=1200)
        channels = [dict(metrics=dict(invalid_responses=0, acknowledged=count)) for count in (2, 0)]
        benchmark = dict(complete=True, start_us=1000, channels=channels, acknowledged=[2, 0])
        common = dict(clock_domain=42, process_usage=dict(user_us=10, system_us=10),
                      clock_simulation=dict(offset_us=0, drift_ppm=0), network=dict(malformed=0))
        self.sender = dict(common, benchmark=benchmark, reliable_benchmark=benchmark)
        self.receiver = dict(common)
        # FNV-1aの1バイト入力0x01/0x02。fixture側は集計コードでhashを作らない。
        self.receipts = [dict(channel=1, sequence=1, received_us=2000, bytes=1, fingerprint=0xAF63BC4C8601B62C),
                         dict(channel=1, sequence=2, received_us=503000, bytes=1, fingerprint=0xAF63BF4C8601BB45)]

    def measured(self, mode="tcp_single", *, allow_incomplete=False):
        for name, report in (("a", self.sender), ("b", self.receiver)):
            (self.directory / f"{name}.report.json").write_text(json.dumps(report))
        (self.directory / "receipts.jsonl").write_text("\n".join(map(json.dumps, self.receipts)))
        return measure(self.directory, self.workload, mode=mode, duration_ms=1000, allow_incomplete=allow_incomplete)

    def test_complete_messages_measure_scheduled_time_to_application_delivery(self):
        metrics = self.measured()["metrics"]
        self.assertEqual(metrics["delivered"], [2, 0])
        self.assertEqual(metrics["short"]["p99_us"], 2000)
        self.assertAlmostEqual(metrics["goodput_mbps"], 16 / 502000)
        self.assertEqual(expected_fingerprint(1, 1), self.receipts[0]["fingerprint"])

    def test_missing_message_is_rejected_even_if_sender_claims_completion(self):
        self.receipts.pop()
        with self.assertRaisesRegex(RuntimeError, "欠落"):
            self.measured()

    def test_duplicate_message_is_rejected(self):
        self.receipts.append(self.receipts[0])
        with self.assertRaisesRegex(RuntimeError, "重複"):
            self.measured()

    def test_wrong_body_is_rejected(self):
        self.receipts[1]["fingerprint"] ^= 1
        with self.assertRaisesRegex(RuntimeError, "本文"):
            self.measured()

    def test_different_clock_domains_cannot_produce_one_way_latency(self):
        self.receiver["clock_domain"] += 1
        with self.assertRaisesRegex(RuntimeError, "namespace"):
            self.measured()

    def test_reordering_is_allowed_only_for_unordered_delivery(self):
        self.receipts.reverse()
        self.receipts[1]["received_us"] = 504000
        with self.assertRaisesRegex(RuntimeError, "順序違反"):
            self.measured("l3_ordered")
        self.assertEqual(self.measured("l3_unordered")["metrics"]["receiver_order_inversions"], [1, 0])

    def test_complete_delivery_with_missing_ack_is_recorded_as_incomplete(self):
        self.sender["benchmark"]["complete"] = False
        self.sender["benchmark"]["acknowledged"] = [1, 0]
        with self.assertRaisesRegex(RuntimeError, "受付"):
            self.measured()
        outcome = self.measured(allow_incomplete=True)
        self.assertFalse(outcome["complete"])
        self.assertTrue(outcome["application_complete"])
        self.assertEqual(outcome["acknowledged"], [1, 0])

    def test_incomplete_trial_does_not_count_undelivered_bytes(self):
        self.sender["benchmark"]["complete"] = False
        self.sender["benchmark"]["acknowledged"] = [1, 0]
        self.receipts.pop()
        outcome = self.measured(allow_incomplete=True)
        self.assertFalse(outcome["application_complete"])
        self.assertAlmostEqual(outcome["metrics"]["goodput_mbps"], 8 / 1000)

    def test_incomplete_trials_are_counted_without_entering_the_successful_median(self):
        success = dict(self.measured(), repetition=1)
        self.sender["benchmark"]["complete"] = False
        self.sender["benchmark"]["acknowledged"] = [1, 0]
        self.receipts[1]["received_us"] += 100000
        incomplete = dict(self.measured(allow_incomplete=True), repetition=2)
        report(self.directory, [success, incomplete])
        summary = json.loads((self.directory / "summary.json").read_text())[0]
        self.assertEqual((summary["completed"], summary["repetitions"]), (1, 2))
        self.assertEqual(summary["short_p99_us"]["median"], 2000)
        self.assertEqual(summary["acknowledged"], [3, 0])

    def test_completion_with_inconsistent_ack_count_is_rejected(self):
        self.sender["benchmark"]["acknowledged"] = [1, 0]
        with self.assertRaisesRegex(RuntimeError, "受付確認数"):
            self.measured()

    def test_simulated_clock_and_delivery_before_schedule_are_rejected(self):
        self.sender["clock_simulation"] = dict(offset_us=10, drift_ppm=0)
        with self.assertRaisesRegex(RuntimeError, "模擬時計"):
            self.measured("l3_unordered")
        self.receipts[0]["received_us"] = 999
        with self.assertRaisesRegex(RuntimeError, "生成予定"):
            self.measured()


if __name__ == "__main__":
    unittest.main()
