package cs451.broadcast;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class UniformReliableBroadcast {

    // Implements Majority ACK algorithm
    private BestEffortBroadcast beb;
//    private Set<Message> delivered;
    private Set<Integer> extra; // Set of sequence numbers bigger than maxContiguous + 1  of received messages
    private int maxContiguous; // Maximum sequence number n such that all messages 1,...,n have been received
    private Set<Message> pending;
    private Map<Message, Set<Integer>> ack;
    private List<Host> hosts;

    public UniformReliableBroadcast(int pid, int sourcePort, InetAddress sourceIp, List<Host> hosts, Map<Integer, Host> idToHost) {
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
        pending.add(m);
        beb.broadcast(m);
    }

    public Message deliver() throws IOException {
        // Implements upon event <beb, Deliver ...>
        Message bebDelivered = beb.deliver();
        if (bebDelivered == null) return null;
        int bebDeliveredSenderId = bebDelivered.getSenderId();
        Set<Integer> bebDeliveredAck = ack.get(bebDelivered);
        if (bebDeliveredAck == null) {
            Set<Integer> singleSet = new HashSet<>();
            singleSet.add(bebDeliveredSenderId);
            ack.put(bebDelivered, singleSet);
        }
        else {
            bebDeliveredAck.add(bebDeliveredSenderId);
            ack.put(bebDelivered, bebDeliveredAck);
        }

        if (!pending.contains(bebDelivered)) {
            pending.add(bebDelivered);
            beb.broadcast(bebDelivered);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
//        for (Pair<Integer, Message> pendingPair : pending) {
//            Message pendingMsg = pendingPair.second;
//            int pendingMsgSeqNum = pendingMsg.getSeqNum();
//            if (canDeliver(pendingMsg) && pendingMsgSeqNum > maxContiguous && !extra.contains(pendingMsgSeqNum)) {
//                // Contiguous message
//                if (pendingMsgSeqNum == maxContiguous + 1) {
//                    // Check if sequence numbers in extra together with received sequence number form a contiguous sequence
//                    int i = 1;
//                    while (extra.contains(pendingMsgSeqNum + i)) {
//                        extra.remove(pendingMsgSeqNum + i);
//                    }
//                    // Minus 1 because the while loop above terminates when it finds first non-contiguous number
//                    maxContiguous = pendingMsgSeqNum + i - 1;
//                }
//                // Non contiguous message
//                else extra.add(pendingMsgSeqNum);
//
//                // Don't think this return is right
//                // Specification implies we should deliver all such messages
//                return pendingMsg;
//            }
//        }

        int bebDeliveredSeqNum = bebDelivered.getSeqNum();
        // Don't need pending contains check?
        if (pending.contains(bebDelivered) && canDeliver(bebDelivered) &&
                bebDeliveredSeqNum > maxContiguous && !extra.contains(bebDeliveredSeqNum)) {
            // Update representation of delivered set
            if (bebDeliveredSeqNum == maxContiguous + 1) {
                // Check if sequence numbers in extra together with received sequence number form a contiguous sequence
                int i = 1;
                while (extra.contains(bebDeliveredSeqNum + i)) {
                    extra.remove(bebDeliveredSeqNum + i);
                }
                // Minus 1 because the while loop above terminates when it finds first non-contiguous number
                maxContiguous = bebDeliveredSeqNum + i - 1;
            }
            // Non contiguous message
            else extra.add(bebDeliveredSeqNum);
            return bebDelivered;
        }

        // Not sure about this
        return null;
    }

    public void stop() {
        beb.stop();
    }
}
