"""Offline regressions for the versioned single-key DELETE comparison."""

import unittest
import xml.etree.ElementTree as ElementTree

from delete_versioned_single_key import parse_version_page, tag_name


PREFIX = ('<?xml version="1.0" encoding="UTF-8"?>'
          '<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">')


class TagNameTests(unittest.TestCase):
    def test_namespaced_tags_reduce_to_the_local_name(self):
        element = ElementTree.fromstring(
            '<Version xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>')
        self.assertEqual(tag_name(element), "Version")
        self.assertEqual(tag_name(ElementTree.fromstring("<DeleteMarker/>")), "DeleteMarker")


class ParseVersionPageTests(unittest.TestCase):
    def test_versions_and_delete_markers_are_counted_separately(self):
        payload = (PREFIX +
                   "<Version><Key>a</Key><VersionId>v1</VersionId></Version>"
                   "<DeleteMarker><Key>a</Key><VersionId>m1</VersionId></DeleteMarker>"
                   "<Version><Key>b</Key><VersionId>v2</VersionId></Version>"
                   "<IsTruncated>false</IsTruncated></ListVersionsResult>").encode()
        versions, markers, truncated, next_key, next_version = parse_version_page(payload)
        self.assertEqual(versions, [{"key": "a", "version_id": "v1"},
                                    {"key": "b", "version_id": "v2"}])
        self.assertEqual(markers, [{"key": "a", "version_id": "m1"}])
        self.assertFalse(truncated)
        self.assertEqual((next_key, next_version), ("", ""))

    def test_truncation_carries_both_next_markers(self):
        payload = (PREFIX +
                   "<DeleteMarker><Key>a</Key><VersionId>m1</VersionId></DeleteMarker>"
                   "<IsTruncated>true</IsTruncated>"
                   "<NextKeyMarker>a</NextKeyMarker>"
                   "<NextVersionIdMarker>m1</NextVersionIdMarker>"
                   "</ListVersionsResult>").encode()
        _, markers, truncated, next_key, next_version = parse_version_page(payload)
        self.assertEqual(markers, [{"key": "a", "version_id": "m1"}])
        self.assertTrue(truncated)
        self.assertEqual((next_key, next_version), ("a", "m1"))

    def test_missing_truncation_flag_is_not_assumed(self):
        payload = (PREFIX +
                   "<Version><Key>a</Key><VersionId>v1</VersionId></Version>"
                   "</ListVersionsResult>").encode()
        _, _, truncated, _, _ = parse_version_page(payload)
        self.assertFalse(truncated)

    def test_malformed_payload_raises_value_error(self):
        with self.assertRaises(ValueError):
            parse_version_page(b"<ListVersionsResult>")

    def test_unknown_entries_are_ignored(self):
        payload = (PREFIX +
                   "<CommonPrefixes><Prefix>p/</Prefix></CommonPrefixes>"
                   "<Version><Key>a</Key><VersionId>v1</VersionId></Version>"
                   "</ListVersionsResult>").encode()
        versions, markers, _, _, _ = parse_version_page(payload)
        self.assertEqual(versions, [{"key": "a", "version_id": "v1"}])
        self.assertEqual(markers, [])


if __name__ == "__main__":
    unittest.main()
