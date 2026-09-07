#!/usr/bin/env python3
"""Focused wire regressions for the disposable recovery fault proxy (stdlib only)."""
import contextlib
import http.client
import http.server
import threading
import unittest

from recovery_remote import FaultProxy


@contextlib.contextmanager
def proxy_response(body=b"", headers=(), length=None):
    class Peer(http.server.BaseHTTPRequestHandler):
        def respond(self):
            self.send_response(200)
            self.send_header("content-length", str(len(body) if length is None else length))
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        do_POST = do_PUT = do_HEAD = respond

        def log_message(self, *_):
            pass

    peer = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Peer)
    peer.daemon_threads = True
    threading.Thread(target=peer.serve_forever, daemon=True).start()
    proxy = FaultProxy(peer.server_port, None)
    connection = http.client.HTTPConnection("127.0.0.1", proxy.server_port, timeout=5)
    try:
        yield connection, proxy
    finally:
        connection.close()
        proxy.release.set()
        proxy.shutdown()
        proxy.server_close()
        peer.shutdown()
        peer.server_close()


class FaultProxyTests(unittest.TestCase):
    def test_xml_control_body_is_opaque_even_with_recursive_entities(self):
        body = (b'<!DOCTYPE InitiateMultipartUploadResult [<!ENTITY recursive "&recursive;">]>'
                b'<InitiateMultipartUploadResult><UploadId>&recursive;</UploadId></InitiateMultipartUploadResult>')
        with proxy_response(body) as (connection, proxy):
            connection.request("POST", "/journal/object?uploads", body=b"")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), body)
            self.assertEqual(proxy.errors, [])

    def test_only_required_fixed_response_headers_are_forwarded(self):
        with proxy_response(headers=(("ETag", '"part-identity"'), ("X-Untrusted-Input", "extra"))) as (connection, _):
            connection.request("PUT", "/journal/object?partNumber=1&uploadId=fixture", body=b"")
            response = connection.getresponse()
            self.assertEqual(response.getheader("etag"), '"part-identity"')
            self.assertIsNone(response.getheader("x-untrusted-input"))
            self.assertEqual(response.read(), b"")

    def test_folded_crlf_response_value_fails_before_headers_are_written(self):
        with proxy_response(headers=(("ETag", '"part"\r\n X-Injected: extra'),)) as (connection, proxy):
            connection.request("PUT", "/journal/object?partNumber=1&uploadId=fixture", body=b"")
            with self.assertRaises(http.client.RemoteDisconnected):
                connection.getresponse()
            self.assertEqual(proxy.errors, ["ValueError"])

    def test_head_retains_the_peer_object_length_without_a_body(self):
        with proxy_response(length=99) as (connection, _):
            connection.request("HEAD", "/journal/object")
            response = connection.getresponse()
            self.assertEqual(response.getheader("content-length"), "99")
            self.assertEqual(response.read(), b"")


if __name__ == "__main__":
    unittest.main()
