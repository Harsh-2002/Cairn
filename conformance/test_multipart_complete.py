"""Offline regressions for the completion-inclusive multipart comparison."""

import hashlib
import unittest
import xml.etree.ElementTree as ElementTree

from multipart_complete import (
    MultipartError,
    OBJECT_BYTES,
    OBJECT_PARTS,
    PART_BYTES,
    complete_body,
    completion_share,
    composite_etag,
    count_listed_keys,
    object_key,
    pair_summary,
    parse_complete_etag,
    parse_upload_id,
    part_payload,
)


class PayloadTests(unittest.TestCase):
    def test_part_buffers_are_exactly_five_mib_and_distinct(self):
        payloads = [part_payload(index) for index in range(OBJECT_PARTS)]
        self.assertTrue(all(len(payload) == PART_BYTES for payload in payloads))
        self.assertEqual(len(set(payloads)), OBJECT_PARTS)
        self.assertEqual(OBJECT_BYTES, OBJECT_PARTS * PART_BYTES)

    def test_part_payloads_are_deterministic(self):
        self.assertEqual(part_payload(3), part_payload(3))
        self.assertNotEqual(part_payload(3), part_payload(4))

    def test_keys_are_distinct_and_share_the_scored_prefix(self):
        keys = [object_key(index) for index in (0, 1, 39)]
        self.assertEqual(len(set(keys)), len(keys))
        self.assertTrue(all(key.startswith("complete-") for key in keys))
        self.assertEqual(keys[0], "complete-000000")


class CompositeEtagTests(unittest.TestCase):
    def test_composite_is_md5_of_concatenated_part_digests(self):
        digests = [hashlib.md5(f"part-{index}".encode()).hexdigest() for index in range(3)]
        expected = hashlib.md5(
            b"".join(bytes.fromhex(digest) for digest in digests)).hexdigest() + "-3"
        self.assertEqual(composite_etag(digests), expected)

    def test_one_part_composite_hashes_the_digest_not_the_body(self):
        digest = hashlib.md5(b"only").hexdigest()
        expected = hashlib.md5(bytes.fromhex(digest)).hexdigest() + "-1"
        self.assertEqual(composite_etag([digest]), expected)
        self.assertNotEqual(composite_etag([digest]), digest + "-1")


class CompleteBodyTests(unittest.TestCase):
    def test_body_round_trips_numbered_parts_in_order(self):
        etags = ['"aaa-1"', '"bbb-2"', '"c&d"']
        root = ElementTree.fromstring(complete_body(etags))
        self.assertEqual(root.tag, "CompleteMultipartUpload")
        parts = list(root)
        self.assertEqual([part.findtext("PartNumber") for part in parts], ["1", "2", "3"])
        self.assertEqual([part.findtext("ETag") for part in parts], etags)

    def test_escaping_keeps_the_xml_well_formed(self):
        payload = complete_body(["a<b>c"])
        self.assertIn(b"a&lt;b&gt;c", payload)
        self.assertEqual(ElementTree.fromstring(payload)[0].findtext("ETag"), "a<b>c")


class ResponseParsingTests(unittest.TestCase):
    def test_upload_id_is_read_from_the_initiate_result(self):
        payload = (b"<?xml version='1.0'?>"
                   b"<InitiateMultipartUploadResult><Bucket>b</Bucket>"
                   b"<Key>k</Key><UploadId>abc-123</UploadId>"
                   b"</InitiateMultipartUploadResult>")
        self.assertEqual(parse_upload_id(payload), "abc-123")

    def test_complete_etag_is_unquoted_after_xml_entity_decoding(self):
        payload = (b"<CompleteMultipartUploadResult>"
                   b"<ETag>&quot;deadbeef-10&quot;</ETag>"
                   b"</CompleteMultipartUploadResult>")
        self.assertEqual(parse_complete_etag(payload), "deadbeef-10")
        literal = b"<CompleteMultipartUploadResult><ETag>\"cafe-3\"</ETag></CompleteMultipartUploadResult>"
        self.assertEqual(parse_complete_etag(literal), "cafe-3")

    def test_malformed_or_unquoted_responses_fail_closed(self):
        for payload in (b"not xml", b"<Result></Result>",
                        b"<Result><ETag>deadbeef</ETag></Result>"):
            with self.assertRaises(MultipartError):
                parse_complete_etag(payload)

    def test_listing_counter_sees_keys_and_truncation(self):
        self.assertEqual(count_listed_keys(b"<ListBucketResult></ListBucketResult>"), (0, False))
        page = (b"<ListBucketResult><IsTruncated>false</IsTruncated>"
                b"<Contents><Key>a</Key></Contents><Contents><Key>b</Key></Contents>"
                b"</ListBucketResult>")
        self.assertEqual(count_listed_keys(page), (2, False))
        truncated = page.replace(b"<IsTruncated>false", b"<IsTruncated>true")
        self.assertEqual(count_listed_keys(truncated), (2, True))


class CompletionShareTests(unittest.TestCase):
    def test_share_is_the_completion_fraction_of_object_time(self):
        self.assertAlmostEqual(completion_share([1.0, 3.0], [0.2, 0.4]), 0.15)

    def test_share_rejects_mismatched_or_empty_samples(self):
        self.assertIsNone(completion_share([], []))
        self.assertIsNone(completion_share([1.0], [0.1, 0.2]))
        self.assertIsNone(completion_share([0.0], [0.0]))
        self.assertIsNone(completion_share([-1.0], [0.1]))


class PairSummaryTests(unittest.TestCase):
    @staticmethod
    def arm(pair, engine, rate, status="PASS", completion_p50=200.0, share=0.2):
        return {"pair": pair, "engine": engine, "objects_per_second": rate,
                "completion": {"p50_ms": completion_p50},
                "completion_share_of_object_time": share, "status": status}

    def test_pairs_join_by_declared_pair_and_report_the_ratio(self):
        arms = [self.arm(1, "rustfs", 40.0), self.arm(1, "cairn", 100.0),
                self.arm(2, "cairn", 80.0), self.arm(2, "rustfs", 50.0)]
        pairs = pair_summary(arms)
        self.assertEqual([item["pair"] for item in pairs], [1, 2])
        self.assertAlmostEqual(pairs[0]["cairn_over_rustfs"], 2.5)
        self.assertAlmostEqual(pairs[1]["cairn_over_rustfs"], 1.6)

    def test_failed_incomplete_or_zero_rate_pairs_are_not_scored(self):
        arms = [self.arm(1, "cairn", 100.0),
                self.arm(1, "rustfs", 10.0, "INCONCLUSIVE"),
                self.arm(2, "cairn", 100.0),
                self.arm(3, "cairn", 100.0), self.arm(3, "rustfs", 0.0)]
        self.assertEqual(pair_summary(arms), [])


if __name__ == "__main__":
    unittest.main()
