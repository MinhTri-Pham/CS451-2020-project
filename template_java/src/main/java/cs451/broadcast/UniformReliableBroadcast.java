package cs451.broadcast;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class UniformReliableBroadcast {

    // Implements Majority ACK algorithm
    private Integer pid; // Pid of broadcaster (represents self in the algorithm)
    private BestEffortBroadcast beb;
//    private Set<Message> delivered;
    private Set<Integer> extra; // Set of sequence numbers bigger than maxContiguous + 1  of received messages
    private int maxContiguous; // Maximum sequence number n such that all messages 1,...,n have been received
    private Set<Pair<Integer, Message>> pending;
    private Map<Message, Set<Integer>> ack;
    private List<Host> hosts;

    public UniformReliableBroadcast(int pid, int sourcePort, InetAddress sourceIp, List<Host> hosts, Map<Integer, Host> idToHost) {
        this.pid = pid;
        this.beb = new BestEffortBroadcast(pid, sourcePort, sourceIp, hosts, idToHost);
        this.extra = new HashSet<>();
        this.maxContiguous = 0;
        this.pending =  new HashSet<>();
        this.ack = new HashMap<>();
        this.hosts = hosts;
    }

    private boolean canDeliver(Message m) {
        return 2*ack.get(m).size() > hosts.size();
    }

    public void broadcast(Message m) throws IOException {
        pending.add(new Pair<>(pid, m));
        beb.broadcast(m);
    }

    // TO DO: resolve this
    public Message deliver() throws IOException {
        // Implements upon event <beb, Deliver ...>
        Message bebDelivered = beb.deliver();
        if (bebDelivered == null) return null;
        int senderId = bebDelivered.getSenderId();
        Set<Integer> bebDeliveredAck = ack.get(bebDelivered);
        if (bebDeliveredAck == null) {
            Set<Integer> singleSet = new HashSet<>();
            singleSet.add(senderId);
            ack.put(bebDelivered, singleSet);
        }
        else {
            bebDeliveredAck.add(senderId);
            ack.put(bebDelivered, bebDeliveredAck);
        }

        Pair<Integer, Message> pair = new Pair<>(senderId, bebDelivered);
        if (!pending.contains(pair)) {
            pending.add(pair);
            beb.broadcast(bebDelivered);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        for (Pair<Integer, Message> pendingPair : pending) {
            Message pendingMsg = pendingPair.second;
            int pendingMsgSeqNum = pendingMsg.getSeqNum();
            if (canDeliver(pendingMsg) && pendingMsgSeqNum > maxContiguous && !extra.contains(pendingMsgSeqNum)) {
                // Contiguous message
                if (pendingMsgSeqNum == maxContiguous + 1) {
                    // Check if sequence numbers in extra together with received sequence number form a contiguous sequence
                    int i = 1;
                    while (extra.contains(pendingMsgSeqNum + i)) {
                        extra.remove(pendingMsgSeqNum + i);
                    }
                    // Minus 1 because the while loop above terminates when it finds first non-contiguous number
                    maxContiguous = pendingMsgSeqNum + i - 1;
                }
                // Non contiguous message
                else extra.add(pendingMsgSeqNum);

                // Don't think this return is right
                // Specification implies we should deliver all such messages
                return pendingMsg;
            }
        }
        // Not sure about this
        return null;
    }

    private static class Pair<A, B> {
        private A first;
        private B second;

        private Pair(A first, B second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair<?, ?> pair = (Pair<?, ?>) o;
            return Objects.equals(first, pair.first) &&
                    Objects.equals(second, pair.second);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }
    }

}
