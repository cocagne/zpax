import os
import os.path
import sys
import unittest

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from zpax import db

class BasicDBTest(unittest.TestCase):

    def setUp(self):
        self.db = db.DB('/tmp/tdb')

    def test_update(self):
        print 'Get invalid: ', self.db.get_value('foo')
        self.db.update_key('foo', 'bar', 5)
