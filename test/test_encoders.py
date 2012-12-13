import os
import os.path
import sys

from twisted.internet import reactor, defer
from twisted.trial import unittest


pd = os.path.dirname

this_dir = pd(os.path.abspath(__file__))

sys.path.append( pd(this_dir) )


from zpax import json_encoder

class JSONEncoderTester(unittest.TestCase):

    def test_json_encoder(self):
        e = json_encoder.JSONEncoder()

        orig = [ dict(foo='bar'), 'baz', 1 ]

        enc = e.encode( 'william', 'wallace', orig )

        from_uid, msg_type, dec = e.decode(enc)

        self.assertEquals( from_uid, 'william' )
        self.assertEquals( msg_type, 'wallace' )
        self.assertEquals( dec, orig )
