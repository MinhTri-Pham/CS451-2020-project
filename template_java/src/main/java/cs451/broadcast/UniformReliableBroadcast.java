package cs451.broadcast;

import cs451.*;

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
        MessageSign bebDeliveredSign = new MessageSign(message.getFirstSenderId(), message.getSeqNum());
        ack.computeIfAbsent(bebDeliveredSign, mSign -> ConcurrentHashMap.newKeySet());
        ack.get(bebDeliveredSign).add(message.getSenderId());

        if (!pending.containsKey(bebDeliveredSign)) {
            pending.put(bebDeliveredSign, message);
            beb.broadcast(new Message(pid, message.getFirstSenderId(), message.getSeqNum(), message.isAck()));
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        Iterator<Map.Entry<MessageSign, Message>> pendingIt = pending.entrySet().iterator();
        while (pendingIt.hasNext()) {
            Map.Entry<MessageSign, Message> entry = pendingIt.next();
            MessageSign pendingSign = entry.getKey();
            if (canDeliver(pendingSign) && !delivered.contains(pendingSign)) {
                delivered.add(pendingSign);
                Message pendingMsg = entry.getValue();
                deliverInterface.deliver(pendingMsg);
                pendingIt.remove(); //garbage clean from pending
            }
        }
    }
}
