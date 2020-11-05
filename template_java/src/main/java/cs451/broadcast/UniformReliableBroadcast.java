package cs451.broadcast;

import cs451.*;

//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
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
//    private List<String> logs = new ArrayList<>();

    private List<Host> hosts;
    private DeliverInterface deliverInterface;

    public UniformReliableBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                                    Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.beb = new BestEffortBroadcast(pid, sourceIp,sourcePort, hosts, idToHost, this);
        this.hosts = hosts;
        this.deliverInterface = deliverInterface;
    }

    private boolean canDeliver(MessageSign messageSign) {
        return 2*ack.getOrDefault(messageSign, ConcurrentHashMap.newKeySet()).size() > hosts.size();
    }

    public void broadcast(Message message) {
        pending.put(new MessageSign(message.getFirstSenderId(), message.getSeqNum()), message);
        beb.broadcast(message);
    }

    @ Override
    public void deliver(Message message) {
        // Implements upon event <beb, Deliver ...>
//        logs.add("BEB delivered " + message + "\n");
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
            beb.broadcast(msg);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        Iterator<MessageSign> pendingIt = pending.keySet().iterator();
        while (pendingIt.hasNext()) {
            MessageSign pendingKey = pendingIt.next();
//            logs.add("Try to URB deliver " + pendingKey + "\n");
            if (canDeliver(pendingKey) && !delivered.contains(pendingKey)) {
                delivered.add(pendingKey);
                deliverInterface.deliver(pending.get(pendingKey));
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

//    public void writeLog() {
//        try {
//            BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("urb_debug_%d.txt", pid)));
//            for (String log : logs) writer.write(log);
//            writer.close();
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

//    public void close() {
//        beb.close();
//    }
}
