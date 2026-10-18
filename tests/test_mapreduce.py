import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from botocore.exceptions import ClientError, EndpointConnectionError
from mapreduce_s3 import count_skills, read_complete_lines, retryable, run
from range_server import range_server
from verify_results import compare_results


class MemoryS3:
    def __init__(self, data):
        self.data = data

    def get_object(self, *, Bucket, Key, Range):
        start, end = map(int, Range.removeprefix("bytes=").split("-"))
        return {"Body": io.BytesIO(self.data[start:end + 1])}


class ChunkTests(unittest.TestCase):
    def check_chunks(self, data, widths):
        for width in widths:
            with self.subTest(width=width):
                chunks = [read_complete_lines(MemoryS3(data), "b", "k", start,
                          min(start + width - 1, len(data) - 1), len(data))
                          for start in range(0, len(data), width)]
                self.assertEqual(b"".join(chunks), data)
                self.assertEqual(sum(len(c.splitlines()) for c in chunks), len(data.splitlines()))

    def test_every_boundary_with_and_without_final_newline(self):
        for data in (b"a\nbbb\ncc\n", b"a\nbbb\ncc", "я\r\nстрока\r\nконец".encode()):
            self.check_chunks(data, range(1, len(data) + 1))

    def test_long_lines_cross_multiple_chunks(self):
        self.check_chunks(b"short\n" + b"x" * 200_000 + b"\nlast", (1024, 65536, 100003))

    def test_empty_object(self):
        self.assertEqual(read_complete_lines(MemoryS3(b""), "b", "k", 0, -1, 0), b"")

    def test_combiner_and_tuple_keys(self):
        lines = [json.dumps({"skills": ["python", "docker", "python", "x\u001fy"]}).encode()] * 3
        rows, baseline = count_skills(lines, False)
        other_rows, optimized = count_skills(lines, True)
        self.assertEqual(rows, other_rows)
        self.assertEqual([Counter(values) for values in baseline], optimized)
        self.assertIn(("x\u001fy",), optimized[0])
        self.assertGreater(sum(map(len, baseline)), sum(map(len, optimized)))

    def test_retry_policy(self):
        self.assertTrue(retryable(EndpointConnectionError(endpoint_url="http://localhost")))
        for status, expected in ((503, True), (429, True), (403, False), (404, False)):
            error = ClientError({"Error": {"Code": str(status)},
                                 "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject")
            self.assertEqual(retryable(error), expected)
        self.assertFalse(retryable(ValueError("invalid JSON")))


class PipelineTests(unittest.TestCase):
    def run_pipeline(self, root, mode="optimized", **options):
        args = argparse.Namespace(key="data.ndjson", out_dir=root, mode=mode, workers=2,
                                  chunk_bytes=40, max_retries=1, inject_failure=False,
                                  summary_out=None)
        vars(args).update(options)
        with contextlib.redirect_stdout(io.StringIO()):
            return run(args)

    def test_process_pool_combiner_and_recovery(self):
        data = b"\n".join(json.dumps({"skills": ["docker", "python", "sql"]}).encode()
                            for _ in range(12))
        with tempfile.TemporaryDirectory() as tmp, range_server(data):
            root = Path(tmp)
            baseline = self.run_pipeline(root / "baseline", "baseline")
            optimized = self.run_pipeline(root / "optimized")
            recovered = self.run_pipeline(root / "recovered", inject_failure=True)
            self.assertEqual(baseline["vacancies"], 12)
            self.assertEqual(optimized["vacancies"], 12)
            self.assertEqual(recovered["retries"], 1)
            self.assertTrue(compare_results(root / "baseline", root / "optimized")["all_equal"])
            self.assertTrue(compare_results(root / "baseline", root / "recovered")["all_equal"])
            with self.assertRaisesRegex(RuntimeError, "chunk 1.*Injected mapper failure"):
                self.run_pipeline(root / "exhausted", inject_failure=True, max_retries=0)

    def test_invalid_json_fails_without_retry(self):
        with tempfile.TemporaryDirectory() as tmp, range_server(b"not-json\n"):
            with self.assertLogs(level="WARNING") as logs:
                with self.assertRaisesRegex(RuntimeError, "chunk 0.*attempt=1"):
                    self.run_pipeline(Path(tmp))
            self.assertEqual(len(logs.output), 1)

    def test_invalid_options_fail_before_s3(self):
        with patch("mapreduce_s3.make_client") as client:
            with self.assertRaises(ValueError):
                self.run_pipeline(Path("unused"), chunk_bytes=0)
            client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
