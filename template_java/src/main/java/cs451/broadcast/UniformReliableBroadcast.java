package cs451.broadcast;

import cs451.*;
import cs451.Observer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UniformReliableBroadcast implements Observer {
    // Implements Majority ACK algorithm
    private int pid;
    private BestEffortBroadcast beb;
    private Set<MessageSign> delivered = ConcurrentHashMap.newKeySet();
    private Map<MessageSign, Message> pending = new ConcurrentHashMap<>();
    private ConcurrentHashMap<MessageSign, Set<Integer>> ack = new ConcurrentHashMap<>();
    private List<Host> hosts;
    private Observer observer;

    public UniformReliableBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                                    Map<Integer, Host> idToHost, Observer observer) {
        this.pid = pid;
        this.beb = new BestEffortBroadcast(pid, sourceIp,sourcePort, hosts, idToHost, this);
        this.hosts = hosts;
        this.observer = observer;
    }

    // Check that the majority of processes have seen this message signature
    private boolean canDeliver(MessageSign messageSign) {
        return 2*ack.getOrDefault(messageSign, ConcurrentHashMap.newKeySet()).size() > hosts.size();
    }

    public void broadcast(Message message) {
        pending.put(new MessageSign(message.getFirstSenderId(), message.getSeqNum()), message);
        beb.broadcast(message);
    }

    // Invoked whenever the underlying beb instance delivers a message
    // Deliver the message if possible under urb semantics
    @ Override
    public void deliver(Message message) {
        // The sender of the message definitely has seen this message
        MessageSign bebDeliveredSign = new MessageSign(message.getFirstSenderId(), message.getSeqNum());
        ack.computeIfAbsent(bebDeliveredSign, mSign -> ConcurrentHashMap.newKeySet());
        ack.get(bebDeliveredSign).add(message.getSenderId());

        // If this is a message with a signature we haven't seen yet, rebroadcast the message to all other processes
        if (!pending.containsKey(bebDeliveredSign)) {
            pending.put(bebDeliveredSign, message);
            beb.broadcast(new Message(pid, message.getFirstSenderId(), message.getSeqNum(), message.isAck(), message.getVc()));
        }
        // Go through all messages (with a unique signature) received by beb instance and deliver them if possible
        Iterator<Map.Entry<MessageSign, Message>> pendingIt = pending.entrySet().iterator();
        while (pendingIt.hasNext()) {
            Map.Entry<MessageSign, Message> entry = pendingIt.next();
            MessageSign pendingSign = entry.getKey();
            // Check the majority condition and that the its message signature is unique
            if (canDeliver(pendingSign) && !delivered.contains(pendingSign)) {
                delivered.add(pendingSign);
                Message pendingMsg = entry.getValue();
                observer.deliver(pendingMsg);
                pendingIt.remove(); //garbage clean from pending
            }
        }
    }
}
