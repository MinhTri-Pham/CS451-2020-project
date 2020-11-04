package cs451.broadcast;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;
import cs451.MessageSign;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class FIFOBroadcast implements DeliverInterface{
    // Implements Broadcast with Sequence Number algorithm
    private int pid;
    private UniformReliableBroadcast urb;
    private AtomicInteger lsn = new AtomicInteger(0);
    private Map<MessageSign, Message> pending = new ConcurrentHashMap<>();
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
        int firstSender = message.getFirstSenderId();
        int seqNum = message.getSeqNum();
        pending.put(new MessageSign(firstSender, seqNum), message);
        Iterator<MessageSign> pendingIt = pending.keySet().iterator();
        while (pendingIt.hasNext()) {
            MessageSign pendingKey = pendingIt.next();
            if (pendingKey.getSeqNum() == next.get(firstSender)) {
                next.incrementAndGet(firstSender);
                deliverInterface.deliver(pending.get(pendingKey));
                pendingIt.remove();
            }
        }
    }

    public void close() {
        urb.close();
    }
}
