import os
import os.path
import sys
import unittest
import random

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append( os.path.dirname(this_dir) )

from zpax import db

class BasicDBTest(unittest.TestCase):

    def setUp(self):
        self.db = db.DB(':memory:')

    def test_update_missing_value(self):
        self.assertTrue(self.db.get_value('foo') is None)
        self.db.update_key('foo', 'bar', 5)
        self.assertEquals(self.db.get_value('foo'), 'bar')

    def test_update_new_value(self):
        self.assertTrue(self.db.get_value('foo') is None)
        self.db.update_key('foo', 'bar', 5)
        self.assertEquals(self.db.get_value('foo'), 'bar')
        self.db.update_key('foo', 'bish', 6)
        self.assertEquals(self.db.get_value('foo'), 'bish')

    def test_update_ignore_previous_proposal(self):
        self.assertTrue(self.db.get_value('foo') is None)
        self.db.update_key('foo', 'bar', 5)
        self.assertEquals(self.db.get_value('foo'), 'bar')
        self.db.update_key('foo', 'baz', 4)
        self.assertEquals(self.db.get_value('foo'), 'bar')

    def test_iter_updates_empty(self):
        l = [ x for x in self.db.iter_updates(100,200) ]
        self.assertEquals(l, [])

    def test_iter_updates_middle(self):
        for x in range(0,10):
            self.db.update_key(str(x), str(x), x)
        l = [ x for x in self.db.iter_updates(1,5) ]
        self.assertEquals(l, [(str(x),str(x),x) for x in range(2,5)])

    def test_iter_updates_ends(self):
        for x in range(0,10):
            self.db.update_key(str(x), str(x), x)
        l = [ x for x in self.db.iter_updates(0,10) ]
        self.assertEquals(l, [(str(x),str(x),x) for x in range(1,10)])

    def test_iter_updates_random_shuffle(self):
        rng = range(0,100)
        random.shuffle(rng)
        for x in rng:
            self.db.update_key(str(x), str(x), x)
        l = [ x for x in self.db.iter_updates(0,100) ]
        self.assertEquals(l, [(str(x),str(x),x) for x in range(1,100)])
        
