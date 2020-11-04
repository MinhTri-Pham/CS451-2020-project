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
    private Set<MessageSign> delivered = ConcurrentHashMap.newKeySet();
    private Map<MessageSign, Message> pending = new ConcurrentHashMap<>();
    private ConcurrentHashMap<MessageSign, Set<Integer>> ack = new ConcurrentHashMap<>();

    private List<Host> hosts;
    private DeliverInterface deliverInterface;

    public UniformReliableBroadcast(int pid, int sourcePort, List<Host> hosts,
                                    Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.beb = new BestEffortBroadcast(pid, sourcePort, hosts, idToHost, this);
        this.hosts = hosts;
        this.deliverInterface = deliverInterface;
    }

    private boolean canDeliver(MessageSign messageSign) {
        int numAcknowledged = ack.getOrDefault(messageSign, ConcurrentHashMap.newKeySet()).size();
        boolean result = 2*numAcknowledged > hosts.size();
        if (result) {
            System.out.println("Can URB deliver " + messageSign);
        }
        else {
            System.out.println(String.format("Could not deliver %s because only %d out of %d acknowledged it",
                    messageSign,numAcknowledged,hosts.size()));
        }
        return result;
    }

    public void broadcast(Message message) {
        System.out.println("URB broadcast " + message);
        pending.put(new MessageSign(message.getFirstSenderId(), message.getSeqNum()), message);
        beb.broadcast(message);
    }

    @ Override
    public void deliver(Message message) {
        // Implements upon event <beb, Deliver ...>
        System.out.println("BEB delivered " + message);
        MessageSign bebDeliveredKey = new MessageSign(message.getFirstSenderId(), message.getSeqNum());
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

        if (!pending.containsKey(bebDeliveredKey)) {
            pending.put(bebDeliveredKey, message);
            Message msg = new Message(pid, message.getFirstSenderId(), message.getSeqNum(), message.isAck());
            System.out.println("BEB broadcast delivered " + msg);
            beb.broadcast(msg);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        Iterator<MessageSign> pendingIt = pending.keySet().iterator();
        while (pendingIt.hasNext()) {
            MessageSign pendingKey = pendingIt.next();
            System.out.println("Try to URB deliver " + pendingKey);
            if (canDeliver(pendingKey) && !delivered.contains(pendingKey)) {
                delivered.add(pendingKey);
                Message pendingMessage = pending.get(pendingKey);
                System.out.println("URB deliver " + pendingMessage);
                deliverInterface.deliver(pendingMessage);
                pendingIt.remove(); //garbage clean from pending
            }
        }

//        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
//        Iterator<MessageFirst> pendingIt = pending.keySet().iterator();
//        while(pendingIt.hasNext()) {
//            MessageFirst pendingKey = pendingIt.next();
//            Message pendingMessage = pending.get(pendingKey);
//            int seqNum = pendingKey.getSeqNum();
//            int firstSender = pendingKey.getFirstSenderId();
//            if (canDeliver(pendingKey) && maxContiguous.get(firstSender) != null
//                    && seqNum > maxContiguous.get(firstSender) && !delivered.contains(pendingKey)) {
//                // Add pendingKey to delivered set
//                if (seqNum == maxContiguous.get(firstSender) + 1) {
//                    // Contiguous
//                    int i = 1;
//                    MessageFirst temp = new MessageFirst(firstSender, seqNum + 1);
//                    while(delivered.contains(temp)) {
//                        delivered.remove(temp);
//                        i++;
//                        temp = new MessageFirst(firstSender, seqNum + 1);
//                    }
//                    // Minus 1 because the while loop above terminates when it finds first non-contiguous number
//                    maxContiguous.put(firstSender, seqNum + i - 1);
//                }
//                else delivered.add(pendingKey);
//                System.out.println("URB deliver " + pendingMessage);
//                deliverInterface.deliver(pendingMessage);
//                pendingIt.remove();
//            }
//        }
    }

    public void close() {
        beb.close();
    }
}
