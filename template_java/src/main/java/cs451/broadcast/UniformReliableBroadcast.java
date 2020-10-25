package cs451.broadcast;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UniformReliableBroadcast implements DeliverInterface {

    // Implements Majority ACK algorithm
    private BestEffortBroadcast beb;
//    private Set<Message> delivered;
    private Set<Integer> extra; // Set of sequence numbers bigger than maxContiguous + 1  of received messages
    private int maxContiguous; // Maximum sequence number n such that all messages 1,...,n have been delivered
    private Set<Message> pending = ConcurrentHashMap.newKeySet();
    private ConcurrentHashMap<Message, Set<Integer>> ack = new ConcurrentHashMap<>();
    private List<Host> hosts;
    private DeliverInterface deliverInterface;

    public UniformReliableBroadcast(int pid, int sourcePort, List<Host> hosts,
                                    Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.beb = new BestEffortBroadcast(pid, sourcePort, hosts, idToHost, this);
        this.extra = new HashSet<>();
        this.maxContiguous = 0;
        this.hosts = hosts;
        this.deliverInterface = deliverInterface;
    }

    private boolean canDeliver(Message m) {
        return 2*ack.get(m).size() > hosts.size();
    }

    public void broadcast(Message m) {
        pending.add(m);
        beb.broadcast(m);
    }

    //TO DO: concurrent access of global variables
    @ Override
    public void deliver(Message message) {
        // Implements upon event <beb, Deliver ...>
        int meesageSendId = message.getSenderId();
        Set<Integer> bebDeliveredAck = ack.get(message);
        if (bebDeliveredAck == null) {
            Set<Integer> singleSet = new HashSet<>();
            singleSet.add(meesageSendId);
            ack.put(message, singleSet);
        }
        else {
            bebDeliveredAck.add(meesageSendId);
            ack.put(message, bebDeliveredAck);
        }

        if (!pending.contains(message)) {
            pending.add(message);
            beb.broadcast(message);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        for (Message pendingMsg : pending) {
            int pendingMsgSeqNum = pendingMsg.getSeqNum();
            if (canDeliver(pendingMsg) && pendingMsgSeqNum > maxContiguous && !extra.contains(pendingMsgSeqNum)) {
                if (pendingMsgSeqNum == maxContiguous + 1) {
                    // Received contiguous message
                    int i = 1;
                    // Check if we have a new a contiguous sequence
                    while (extra.contains(pendingMsgSeqNum + i)) {
                        extra.remove(pendingMsgSeqNum + i);
                    }
                    // Minus 1 because the while loop above terminates when it finds first non-contiguous number
                    maxContiguous = pendingMsgSeqNum + i - 1;
                }
                // Non contiguous message
                else extra.add(pendingMsgSeqNum);
                deliverInterface.deliver(pendingMsg);
                pending.remove(pendingMsg); // Garbage clean pending
            }
        }

    }

//    public Message deliver() throws IOException {
//        // Implements upon event <beb, Deliver ...>
//        Message bebDelivered = beb.deliver();
//        if (bebDelivered == null) return null;
//        int bebDeliveredSenderId = bebDelivered.getSenderId();
//        Set<Integer> bebDeliveredAck = ack.get(bebDelivered);
//        if (bebDeliveredAck == null) {
//            Set<Integer> singleSet = new HashSet<>();
//            singleSet.add(bebDeliveredSenderId);
//            ack.put(bebDelivered, singleSet);
//        }
//        else {
//            bebDeliveredAck.add(bebDeliveredSenderId);
//            ack.put(bebDelivered, bebDeliveredAck);
//        }
//
//        if (!pending.contains(bebDelivered)) {
//            pending.add(bebDelivered);
//            beb.broadcast(bebDelivered);
//        }
//
//        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
//        int bebDeliveredSeqNum = bebDelivered.getSeqNum();
//        // Don't need pending contains check?
//        if (pending.contains(bebDelivered) && canDeliver(bebDelivered) &&
//                bebDeliveredSeqNum > maxContiguous && !extra.contains(bebDeliveredSeqNum)) {
//            // Update representation of delivered set
//            if (bebDeliveredSeqNum == maxContiguous + 1) {
//                // Check if sequence numbers in extra together with received sequence number form a contiguous sequence
//                int i = 1;
//                while (extra.contains(bebDeliveredSeqNum + i)) {
//                    extra.remove(bebDeliveredSeqNum + i);
//                }
//                // Minus 1 because the while loop above terminates when it finds first non-contiguous number
//                maxContiguous = bebDeliveredSeqNum + i - 1;
//            }
//            // Non contiguous message
//            else extra.add(bebDeliveredSeqNum);
//            return bebDelivered;
//        }
//
//        // Not sure about this
//        return null;
//    }

    public void close() {
        beb.close();
    }
}
