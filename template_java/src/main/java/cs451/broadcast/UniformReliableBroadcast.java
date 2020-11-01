package cs451.broadcast;

import cs451.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UniformReliableBroadcast implements DeliverInterface {

    // Implements Majority ACK algorithm
    private int pid; // Pid of broadcaster
    private BestEffortBroadcast beb;
    // To compress representation of delivered set. For each first sender,
    // store sequence number n such that messages with sequence number 1,..., n have been delivered
    private Map<Integer, Integer> maxContiguous = new ConcurrentHashMap<>();
    private Set<MessageFirst> delivered = ConcurrentHashMap.newKeySet();
    private Map<MessageFirst, Message> pending = new ConcurrentHashMap<>();
    private ConcurrentHashMap<MessageFirst, Set<Integer>> ack = new ConcurrentHashMap<>();

//    private Set<Message> delivered = ConcurrentHashMap.newKeySet();
//    private Set<Message> pending = ConcurrentHashMap.newKeySet();
//    private ConcurrentHashMap<Message, Set<Integer>> ack = new ConcurrentHashMap<>();

    private List<Host> hosts;
    private DeliverInterface deliverInterface;

    public UniformReliableBroadcast(int pid, int sourcePort, List<Host> hosts,
                                    Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.beb = new BestEffortBroadcast(pid, sourcePort, hosts, idToHost, this);
        this.hosts = hosts;
        this.deliverInterface = deliverInterface;
    }

    private boolean canDeliver(MessageFirst messageFirst) {
        return 2*ack.getOrDefault(messageFirst, ConcurrentHashMap.newKeySet()).size() > hosts.size();
    }

    public void broadcast(Message message) {
        System.out.println("URB broadcast " + message);
        pending.put(new MessageFirst(message.getFirstSenderId(), message.getSeqNum()), message);
        System.out.println("pending: " + pending);
        System.out.println("BEB broadcast " + message);
        beb.broadcast(message);
    }

    @ Override
    public void deliver(Message message) {
        // Implements upon event <beb, Deliver ...>
        System.out.println("BEB delivered " + message);
        MessageFirst bebDeliveredKey = new MessageFirst(message.getFirstSenderId(), message.getSeqNum());
        Set<Integer> bebDeliveredAck = ack.get(bebDeliveredKey);
        if (bebDeliveredAck == null) {
            Set<Integer> singleSet = ConcurrentHashMap.newKeySet();
            singleSet.add(message.getSenderId());
            ack.put(bebDeliveredKey, singleSet);
        }
        else {
            bebDeliveredAck.add(message.getSenderId());
            ack.put(bebDeliveredKey, bebDeliveredAck);
        }
        System.out.println("pending: " + pending);
        System.out.println("ack: " + ack);

        if (!pending.containsKey(bebDeliveredKey)) {
            pending.put(bebDeliveredKey, message);
            System.out.println("pending: " + pending);
            Message msg = new Message(pid, message.getFirstSenderId(), message.getSeqNum(), message.isAck());
            System.out.println("BEB broadcast delivered" + msg);
            beb.broadcast(msg);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        Iterator<MessageFirst> pendingIt = pending.keySet().iterator();
        while(pendingIt.hasNext()) {
            MessageFirst pendingKey = pendingIt.next();
            Message pendingMessage = pending.get(pendingKey);
            int seqNum = pendingKey.getSeqNum();
            int firstSender = pendingKey.getFirstSenderId();
            if (canDeliver(pendingKey) && maxContiguous.get(firstSender) != null
                    && seqNum > maxContiguous.get(firstSender) && !delivered.contains(pendingKey)) {
                // Add pendingKey to delivered set
                if (seqNum == maxContiguous.get(firstSender) + 1) {
                    // Contiguous
                    int i = 1;
                    MessageFirst temp = new MessageFirst(firstSender, seqNum + 1);
                    while(delivered.contains(temp)) {
                        delivered.remove(temp);
                        i++;
                        temp = new MessageFirst(firstSender, seqNum + 1);
                    }
                    // Minus 1 because the while loop above terminates when it finds first non-contiguous number
                    maxContiguous.put(firstSender, seqNum + i - 1);
                }
                else delivered.add(pendingKey);
                System.out.println("URB deliver " + message);
                deliverInterface.deliver(pendingMessage);
                pendingIt.remove();
            }
        }

//        for (Message pendingMsg : pending) {
//            int pendingMsgSeqNum = pendingMsg.getSeqNum();
//            int pendingMsgSenderId = pendingMsg.getSenderId();
//            if (canDeliver(pendingMsg) && maxContiguous.get(pendingMsgSenderId) != null
//                    && pendingMsgSeqNum > maxContiguous.get(pendingMsgSenderId) && !delivered.contains(pendingMsg)) {
//                // Add pendingMsg to delivered set
//                if (pendingMsgSeqNum == maxContiguous.get(pendingMsg.getSenderId() + 1)) {
//                    // Contiguous message
//                    int i = 1;
//                    Message temp = new Message(pendingMsgSenderId, pendingMsgSeqNum + i, false);
//                    // Check if we have a new a contiguous sequence
//                    while (delivered.contains(temp)) {
//                        delivered.remove(temp);
//                        i++;
//                        temp = new Message(pendingMsgSenderId, pendingMsgSeqNum + i, false);
//                    }
//                    // Minus 1 because the while loop above terminates when it finds first non-contiguous number
//                    maxContiguous.put(pendingMsgSenderId, pendingMsgSeqNum + i - 1);
//                }
//                // Non-contiguous message
//                else delivered.add(pendingMsg);
//                deliverInterface.deliver(pendingMsg);
//                pending.remove(pendingMsg); // Garbage clean pending
//            }
//        }
    }

    public void close() {
        beb.close();
    }
}
