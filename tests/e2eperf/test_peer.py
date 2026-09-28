"""Exercise receipt framing over real persistent HTTP connections."""
import http.client
import json
from pathlib import Path
import tempfile
import unittest

from run import Peer, body, digest


class PeerFramingTest(unittest.TestCase):
    def test_success_receipts_have_byte_lengths_and_reuse_connection(self):
        with tempfile.TemporaryDirectory() as temp:
            peer = Peer(Path(temp)/'ledger', {'token': 'local-test', 'payload': 8, 'count': 4})
            conn = http.client.HTTPConnection('127.0.0.1', peer.server.server_port, timeout=2)
            try:
                for identity in (1, 2):
                    wire = json.dumps({'id': identity, 'body': body(identity, 8)}).encode()
                    conn.request('POST', '/seed', wire, {'Authorization': 'Bearer local-test',
                                                        'Content-Type': 'application/json'})
                    response = conn.getresponse()
                    self.assertEqual(response.status, 200)
                    declared = response.getheader('Content-Length')
                    self.assertTrue(declared.isascii() and declared.isdecimal(), declared)
                    raw = response.read()
                    self.assertEqual(int(declared), len(raw))
                    self.assertEqual(json.loads(raw), {'id': identity, 'hash': digest(body(identity, 8).encode()),
                                                       'durable': True})
                self.assertEqual([row['id'] for row in peer.rows], [1, 2])
                self.assertEqual(peer.errors, [])
            finally:
                conn.close()
                peer.close()

    def test_error_receipt_is_framed_and_never_enters_ledger(self):
        with tempfile.TemporaryDirectory() as temp:
            peer = Peer(Path(temp)/'ledger', {'token': 'local-test', 'payload': 8, 'count': 4})
            conn = http.client.HTTPConnection('127.0.0.1', peer.server.server_port, timeout=2)
            try:
                conn.request('POST', '/seed', b'{}', {'Authorization': 'Bearer wrong',
                                                      'Content-Type': 'application/json'})
                response = conn.getresponse()
                self.assertEqual(response.status, 422)
                declared = response.getheader('Content-Length')
                self.assertTrue(declared.isascii() and declared.isdecimal(), declared)
                raw = response.read()
                self.assertEqual(int(declared), len(raw))
                self.assertEqual(json.loads(raw), {'error': 'peer auth'})
                self.assertEqual(peer.rows, [])
            finally:
                conn.close()
                peer.close()
