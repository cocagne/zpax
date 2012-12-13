import json

from twisted.python import log


class JSONEncoder (object):

    def encode(self, node_uid, message_type, parts):
        header = dict()

        header['type'] = message_type
        header['from'] = node_uid

        plist = [ header ]
        
        if parts:
            plist.extend( parts )
        
        return [ json.dumps(p) for p in plist ]


    def decode(self, jparts):
        if not jparts:
            return
        
        try:
            parts = [ json.loads(j) for j in jparts ]
        except ValueError:
            print 'Invalid JSON: ', jparts
            return

        header = parts[0]
        parts  = parts[1:]

        return header['from'], header['type'], parts
