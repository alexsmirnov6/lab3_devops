import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from duckdb_analysis import regional_reports
from s3_io import publish_results


class RegionalTests(unittest.TestCase):
    def test_frequencies_denominators_ties_and_empty_region(self):
        with duckdb.connect() as con:
            con.execute("CREATE TABLE vacancies(vacancy_id VARCHAR, area VARCHAR, skills VARCHAR[], salary_mid_rur_gross DOUBLE)")
            con.executemany("INSERT INTO vacancies VALUES (?, ?, ?, ?)", [
                ("1", "A", ["a", "b", "c"], 100),
                ("2", "A", ["a", "b"], 200), ("3", "A", [], None),
                ("4", "B", [], None),
            ])
            con.execute("CREATE TABLE skill_rows AS SELECT vacancy_id, area, unnest(skills) AS skill FROM vacancies")
            con.execute("CREATE TABLE pair_rows AS SELECT a.area, a.skill AS skill1, b.skill AS skill2 FROM skill_rows a JOIN skill_rows b USING(vacancy_id) WHERE a.skill < b.skill")
            con.execute("CREATE TABLE triple_rows AS SELECT a.area, a.skill AS skill1, b.skill AS skill2, c.skill AS skill3 FROM skill_rows a JOIN skill_rows b USING(vacancy_id) JOIN skill_rows c USING(vacancy_id) WHERE a.skill < b.skill AND b.skill < c.skill")
            result = regional_reports(con, ["A", "B"])
            self.assertEqual(result["summary"][0]["vacancies"], 3)
            pairs = result["top_pairs"]
            self.assertEqual(pairs[0]["skills"], ["a", "b"])
            self.assertEqual(pairs[0]["vacancies"], 2)
            self.assertEqual(pairs[0]["share_pct"], 100)
            self.assertEqual(pairs[0]["share_all_vacancies_pct"], 66.67)
            self.assertEqual([p["skills"] for p in pairs[1:]], [["a", "c"], ["b", "c"]])
            self.assertEqual(result["top_triples"][0]["share_pct"], 50)
            self.assertEqual(result["top_triples"][0]["share_all_vacancies_pct"], 33.33)
            self.assertFalse(any(p["region"] == "B" for p in pairs))
            self.assertEqual(regional_reports(con, [])["summary"], [])


class PublicationTests(unittest.TestCase):
    def test_downloaded_bytes_are_checked_and_missing_input_prevents_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bodies = {}
            def put(s3, bucket, path, key):
                bodies[key] = path.read_bytes()
                return {"key": key, "bytes": len(bodies[key]),
                        "sha256_metadata": hashlib.sha256(bodies[key]).hexdigest()}
            s3 = Mock()
            s3.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(bodies[kw["Key"]])}
            s3.get_paginator.return_value.paginate.return_value = [{}]
            with patch("s3_io.upload", side_effect=put) as upload:
                with self.assertRaises(FileNotFoundError):
                    publish_results(s3, "bucket", root, "results/mapreduce")
                upload.assert_not_called()
                for width, name in enumerate(("skills", "pairs", "triples"), 1):
                    (root / f"{name}.json").write_text(json.dumps({
                        "stack_size": width, "counts": [], "unique_combinations": 0, "vacancies": 0,
                    }), encoding="utf-8")
                result = publish_results(s3, "bucket", root, "results/mapreduce")
                self.assertTrue(result["all_verified"])
                self.assertEqual(len(result["objects"]), 3)
                self.assertTrue(all(obj["verified"] for obj in result["objects"]))
                s3.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(b"corrupted")}
                with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                    publish_results(s3, "bucket", root, "results/mapreduce")
