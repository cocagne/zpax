


class JSONMessenger (object):

    def __init__(self, network_node):
        self.nn = network_node
        

    def encode(self, message_type, *parts, **header_attrs):
        header = dict()

        header['type'] = message_type
        header['from'] = self.nn.node_uid
        
        header.extend(header_attrs)

        plist = [ header ]
        
        if parts:
            plist.extend( parts )
        
        return [ json.dumps(p) for p in plist ]

        
