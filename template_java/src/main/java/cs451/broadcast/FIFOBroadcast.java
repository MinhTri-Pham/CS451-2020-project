package cs451.broadcast;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FIFOBroadcast {
    private UniformReliableBroadcast urb;
    private int lsn;
    private Set<Message> pending;
    private int[] next;

    public FIFOBroadcast(int pid, int sourcePort, InetAddress sourceIp, List<Host> hosts) {
        // Implements Broadcast with Sequence Number algorithm
        this.urb = new UniformReliableBroadcast(pid, sourcePort, sourceIp, hosts);
        this.lsn = 1;
        this.pending = new HashSet<>();
        this.next = new int[hosts.size()];
        Arrays.fill(next,1);
    }

    public void broadcast(Message m) throws IOException {
        lsn++;
        urb.broadcast(m.withSeqNum(lsn));
    }

    public void deliver(Message m) {
        pending.add(m);
        for (Message pendingMsg : pending) {
            if (next[pendingMsg.getSenderId()] == pendingMsg.getSeqNum()) {
                next[pendingMsg.getSenderId()]++;
                pending.remove(pendingMsg);
            }
        }
        // Deliver
    }

}
