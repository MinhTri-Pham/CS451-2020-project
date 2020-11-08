package cs451.broadcast;

import cs451.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UniformReliableBroadcast implements DeliverInterface {

    // Implements Majority ACK algorithm
    private int pid; // Pid of broadcaster
    private BestEffortBroadcast beb;
//    private Map<Integer, Integer> maxContiguous = new ConcurrentHashMap<>();
    private Set<MessageSign> delivered = ConcurrentHashMap.newKeySet();
    private Map<MessageSign, Message> pending = new ConcurrentHashMap<>();
    private ConcurrentHashMap<MessageSign, Set<Integer>> ack = new ConcurrentHashMap<>();
    private List<String> logs = new ArrayList<>();

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
        logs.add(String.format("ack of message trying to deliver: %s\n", ack.getOrDefault(messageSign, ConcurrentHashMap.newKeySet())));
        System.out.println("ack of message trying to deliver: " + ack.getOrDefault(messageSign, ConcurrentHashMap.newKeySet()));
        return 2*ack.getOrDefault(messageSign, ConcurrentHashMap.newKeySet()).size() > hosts.size();
    }

    public void broadcast(Message message) {
        pending.put(new MessageSign(message.getFirstSenderId(), message.getSeqNum()), message);
        beb.broadcast(message);
    }

    @ Override
    public void deliver(Message message) {
        // Implements upon event <beb, Deliver ...>
        logs.add(String.format("BEB delivered %s\n", message));
        System.out.println("BEB delivered " + message);
        MessageSign bebDeliveredSign = new MessageSign(message.getFirstSenderId(), message.getSeqNum());
        ack.computeIfAbsent(bebDeliveredSign, mSign -> ConcurrentHashMap.newKeySet());
        ack.get(bebDeliveredSign).add(message.getSenderId());
        logs.add(String.format("Ack of BEB delivered %s\n", ack.get(bebDeliveredSign)));
        System.out.println("Ack of BEB delivered " + ack.get(bebDeliveredSign));

        if (!pending.containsKey(bebDeliveredSign)) {
            pending.put(bebDeliveredSign, message);
            Message toBroadcast = new Message(pid, message.getFirstSenderId(), message.getSeqNum(), message.isAck());
            logs.add(String.format("Rebroadcast  %s\n", toBroadcast));
            System.out.println("Rebroadcast " + toBroadcast);
            beb.broadcast(toBroadcast);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        Iterator<Map.Entry<MessageSign, Message>> pendingIt = pending.entrySet().iterator();
        while (pendingIt.hasNext()) {
            Map.Entry<MessageSign, Message> entry = pendingIt.next();
            MessageSign pendingSign = entry.getKey();
            Message pendingMsg = entry.getValue();
            logs.add(String.format("Try to deliver msg with signature %s\n", pendingSign));
            System.out.println("Try to deliver msg with signature" + pendingSign);
            if (canDeliver(pendingSign) && !delivered.contains(pendingSign)) {
                delivered.add(pendingSign);
                deliverInterface.deliver(pendingMsg);
                logs.add(String.format("URB delivered %s\n", pendingMsg));
                System.out.println("URB delivered " + pendingMsg);
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

    public void writeLog() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("urb_debug_%d.txt", pid)));
            for (String log : logs) writer.write(log);
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        writeLog();
        beb.close();
    }
}
