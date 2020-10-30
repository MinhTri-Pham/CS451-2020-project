package cs451.broadcast;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class FIFOBroadcast implements DeliverInterface{
    // Implements Broadcast with Sequence Number algorithm
    private int pid;
    private UniformReliableBroadcast urb;
    private AtomicInteger lsn = new AtomicInteger(0);
    private Set<Message> pending = ConcurrentHashMap.newKeySet();;
    private AtomicIntegerArray next;
    private DeliverInterface deliverInterface;

    public FIFOBroadcast(int pid, int sourcePort, List<Host> hosts,
                         Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.urb = new UniformReliableBroadcast(pid, sourcePort, hosts, idToHost, this);
        this.deliverInterface = deliverInterface;
        this.next = new AtomicIntegerArray(hosts.size());
    }

    public void broadcast(Message message){
        urb.broadcast(new Message(pid, message.getFirstSenderId(), lsn.incrementAndGet(), message.isAck()));
    }

    @Override
    public void deliver(Message message) {
        pending.add(message);
        for (Message pendingMsg : pending) {
            if (next.get(pendingMsg.getSenderId()) == pendingMsg.getSeqNum()) {
                next.incrementAndGet(pendingMsg.getSenderId());
                pending.remove(pendingMsg);
                deliverInterface.deliver(pendingMsg);
            }
        }
    }

    public void close() {
        urb.close();
    }
}
