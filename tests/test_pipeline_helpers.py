import contextlib
import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from botocore.exceptions import ClientError
from prepare_dataset import prepare
from profile_pipeline import aggregate, execute
from s3_io import ensure_bucket


class HelperTests(unittest.TestCase):
    def test_prepare_normalizes_and_handles_invalid_salary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.csv"
            with source.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["vacancy_id", "name", "key_skills",
                    "currency", "salary_from", "salary_to", "gross"])
                writer.writeheader()
                writer.writerow({"vacancy_id": "1", "name": "DevOps", "key_skills": "Python; python ; Docker",
                                 "currency": "RUR", "salary_from": "bad", "salary_to": "100", "gross": "true"})
                writer.writerow({"vacancy_id": "2", "name": "Designer"})
            with contextlib.redirect_stdout(io.StringIO()):
                metadata = prepare(source, root / "prepared")
            record = json.loads((root / "prepared/devops_vacancies.ndjson").read_text(encoding="utf-8"))
            self.assertEqual(metadata["selected_rows"], 1)
            self.assertEqual(record["skills"], ["docker", "python"])
            self.assertIsNone(record["salary_from"])
            self.assertEqual(record["salary_mid_rur_gross"], 100)

    def test_even_number_of_measurements_uses_mean_of_middle_values(self):
        result = aggregate([{"wall_seconds": 1, "peak_rss_mib": 10},
                            {"wall_seconds": 3, "peak_rss_mib": 30}])
        self.assertEqual(result["wall_seconds_median"], 2)
        self.assertEqual(result["peak_rss_mib_median"], 20)

    def test_profiler_drains_large_output(self):
        result = execute([sys.executable, "-c",
            "import json; print(json.dumps(dict(elapsed_seconds=0, vacancies=1, outputs={}, padding='x'*200000)))"])
        self.assertEqual(result["vacancies"], 1)

    def test_bucket_permission_error_is_not_treated_as_missing(self):
        client = Mock()
        client.head_bucket.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadBucket")
        with self.assertRaises(ClientError):
            ensure_bucket(client, "bucket")
        client.create_bucket.assert_not_called()
        client.head_bucket.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadBucket")
        ensure_bucket(client, "bucket")
        client.create_bucket.assert_called_once_with(Bucket="bucket")
