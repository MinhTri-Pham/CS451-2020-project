package cs451.broadcast;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class FIFOBroadcast {
    private UniformReliableBroadcast urb;
    private int lsn;
    private Set<Message> pending;
    private int[] next;

    public FIFOBroadcast(int pid, int sourcePort, InetAddress sourceIp, List<Host> hosts, Map<Integer, Host> idToHost) {
        // Implements Broadcast with Sequence Number algorithm
        this.urb = new UniformReliableBroadcast(pid, sourcePort, sourceIp, hosts, idToHost);
        this.lsn = 1;
        this.pending = new HashSet<>();
        this.next = new int[hosts.size()];
        Arrays.fill(next,1);
    }

    public void broadcast(Message m) throws IOException {
        lsn++;
        urb.broadcast(m.withSeqNum(lsn));
    }

    public Message deliver() throws IOException {
        Message urbDelivered = urb.deliver();
        pending.add(urbDelivered);
        for (Message pendingMsg : pending) {
            if (next[pendingMsg.getSenderId()] == pendingMsg.getSeqNum()) {
                next[pendingMsg.getSenderId()]++;
                pending.remove(pendingMsg);
                // Don't think this return is right
                // Specification implies we should deliver all such messages
                return pendingMsg;
            }
        }
        // Not sure about this
        return null;
    }

}
