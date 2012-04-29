import os.path
import sqlite3

tables = dict()

tables['kv'] = '''
key      text PRIMARY KEY,
value    text,
proposal integer
'''

class DB (object):

    def __init__(self, fn):
        self._fn = fn
        create = not os.path.exists(fn)

        
        self._con = sqlite3.connect(fn)
        self._cur = self._con.cursor()

        if create:
            self.create_db()

            
    def create_db(self):
        cur = self._con.cursor()

        for k,v in tables.iteritems():
            cur.execute('create table {} ({})'.format(k,v))

        cur.execute('create index proposal_index on kv (proposal)')

        self._con.commit()
        cur.close()


    def get_value(self, key):
        r = self._cur.execute('SELECT value FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def get_proposal(self, key):
        r = self._cur.execute('SELECT proposal FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def update_key(self, key, value, proposal_number):
        prevpn = self.get_proposal(key)
        if prevpn and proposal_number > prevpn:
            self._cur.execute('UPDATE kv SET value=?, proposal=? WHERE key=?',
                              (value, proposal_number, key))
            self._con.commit()
            
        elif prevpn is None:
            self._cur.execute('INSERT INTO kv VALUES (?, ?, ?)',
                              (key, value, proposal_number))
            self._con.commit()

            
    def get_last_proposal(self):
        return self._cur.execute('SELECT MAX(proposal) FROM kv').fetchone()[0]

    
    def iter_updates(self, start_proposal, end_proposal=2**32):
        c = self._con.cursor()
        c.execute('SELECT key,value,proposal FROM kv WHERE proposal>? AND proposal<?  ORDER BY proposal',
                  (start_proposal, end_proposal))
        return c
