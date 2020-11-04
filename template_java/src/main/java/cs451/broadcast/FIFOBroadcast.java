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
    private AtomicInteger lsn = new AtomicInteger(1);
    private Map<MessageSign, Message> pending = new ConcurrentHashMap<>();
    private AtomicIntegerArray next;
    private DeliverInterface deliverInterface;

    public FIFOBroadcast(int pid, int sourcePort, List<Host> hosts,
                         Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.urb = new UniformReliableBroadcast(pid, sourcePort, hosts, idToHost, this);
        this.deliverInterface = deliverInterface;

        int[] nextTmp = new int[hosts.size() + 1];
        Arrays.fill(nextTmp, 1);
        this.next = new AtomicIntegerArray(nextTmp);
    }

    public void broadcast(Message message){
        Message toSend = new Message(pid, message.getFirstSenderId(), lsn.getAndIncrement(), message.isAck());
        urb.broadcast(toSend);
        System.out.println("FIFO broadcast " + toSend);

    }

    @Override
    public void deliver(Message message) {
        System.out.println("URB delivered " + message);
        int firstSender = message.getFirstSenderId();
        int seqNum = message.getSeqNum();
        System.out.println(next);
        // No point trying to deliver if got message with sequence number < next sequence to be delivered
        // process withg id firstSender
        if (seqNum >= next.get(firstSender)) {
            pending.put(new MessageSign(firstSender, seqNum), message);
            Iterator<MessageSign> pendingIt = pending.keySet().iterator();
            while (pendingIt.hasNext()) {
                MessageSign pendingKey = pendingIt.next();
                if (pendingKey.getSeqNum() == next.get(firstSender)) {
                    next.incrementAndGet(firstSender);
                    Message pendingMsg = pending.get(pendingKey);
                    deliverInterface.deliver(pendingMsg);
                    System.out.println("FIFO deliver: " + pendingMsg);
                    pendingIt.remove();
                }
            }
        }
    }

    public void close() {
        urb.close();
    }
}
