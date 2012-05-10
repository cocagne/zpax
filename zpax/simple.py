
from zpax import tzmq
from paxos import multi
from paxos.leaders import heartbeat

class SimpleHeartbeatProposer (heartbeat.Proposer):
    hb_period       = 500
    liveness_window = 1500

    def __init__(self, simple_node):
        self.node = simple_node

        super(SimpleHeartbeatProposer, self).__init__(self.node.local_ps_addr,
                                                      self.node.paxos_threshold,
                                                      leader_uid = self.node.current_leader)

    def send_prepare(self, proposal_num):
        self.node.paxos_send_prepare(proposal_num)

    def send_accept(self, proposal_num, proposal_value):
        self.node.paxos_send_accept(proposal_num, proposal_value)

    def send_heartbeat(self, leader_proposal_number):
        self.node.paxos_send_heartbeat(leader_proposal_number)

    def schedule(self, msec_delay, func_obj):
        self.node.paxos_schedule(msec_delay, func_obj)

    #def on_leadership_acquired(self):
    #    self.node.paxos_on_leadership_acquired()

    #def on_leadership_lost(self):
    #    self.node.paxos_on_leadership_lost(self.leader_uid)
    



class SimpleNode (object):

    def __init__(self,
                 local_pub_sub_addr,   local_rep_addr,
                 remote_pub_sub_addrs, remote_rep_addrs,
                 paxos_threshold,
                 initial_value='', sequence_number=0):
        
        self.local_ps_addr    = local_pub_sub_addr
        self.local_rep_addr   = local_rep_addr
        self.remote_ps_addrs  = remote_pub_sub_addrs
        self.remote_rep_addrs = remote_rep_addrs
        self.paxos_threshold  = paxos_threshold
        self.value            = initial_value
        self.sequence_number  = sequence_number
        self.is_valid         = False
        self.current_leader   = None

        self.mpax             = multi.MultiPaxos(local_pub_sub_addr,
                                                 paxos_threshold,
                                                 0)
        self.mpax.node_factory = self._node_factory
        
        self.pubsub      = tzmq.ZmqPubSocket()
        self.rep         = tzmq.ZmqRepSocket()
        self.req         = tzmq.ZmqReqSocket()

        self.pubsub.messageReceived = self.onPubSubReceived
        self.rep.messageReceived    = self.onRepReceived
        self.req.messageReceived    = self.onReqReceived

        self.pubsub.bind(self.local_ps_addr)
        self.rep.bind(self.local_rep_addr)

        for x in remote_pub_sub_addrs:
            self.pubsub.connect(x)

        for x in remote_rep_addrs:
            self.req.connect(x)


    def _node_factory(self,  quorum_size, resolution_callback):
        return basic.Node( SimpleHeartbeatProposer(self),
                           basic.Acceptor(),
                           basic.Learner(quorum_size),
                           resolution_callback )


    def onPubSubReceived(self, msg_parts):
        pass

    def onRepReceived(self, msg_parts):
        pass

    def onReqReceived(self, msg_parts):
        pass
        
