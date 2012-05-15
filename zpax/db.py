import os.path
import sqlite3

tables = dict()

tables['kv'] = '''
key      text PRIMARY KEY,
value    text,
resolution integer
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

        cur.execute('create index resolution_index on kv (resolution)')

        self._con.commit()
        cur.close()


    def get_value(self, key):
        r = self._cur.execute('SELECT value FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def get_resolution(self, key):
        r = self._cur.execute('SELECT resolution FROM kv WHERE key=?', (key,)).fetchone()
        if r:
            return r[0]

        
    def update_key(self, key, value, resolution_number):
        prevpn = self.get_resolution(key)
        if prevpn and resolution_number > prevpn:
            self._cur.execute('UPDATE kv SET value=?, resolution=? WHERE key=?',
                              (value, resolution_number, key))
            self._con.commit()
            
        elif prevpn is None:
            self._cur.execute('INSERT INTO kv VALUES (?, ?, ?)',
                              (key, value, resolution_number))
            self._con.commit()

            
    def get_last_resolution(self):
        return self._cur.execute('SELECT MAX(resolution) FROM kv').fetchone()[0]

    
    def iter_updates(self, start_resolution, end_resolution=2**32):
        c = self._con.cursor()
        c.execute('SELECT key,value,resolution FROM kv WHERE resolution>? AND resolution<?  ORDER BY resolution',
                  (start_resolution, end_resolution))
        return c
