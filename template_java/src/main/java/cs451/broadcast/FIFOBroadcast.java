package cs451.broadcast;

import cs451.Observer;
import cs451.Host;
import cs451.Message;
import cs451.MessageSign;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class FIFOBroadcast implements Observer {
    // Implements Broadcast with Sequence Number algorithm
    private int pid;
    private UniformReliableBroadcast urb;
    private AtomicInteger lsn = new AtomicInteger(1);
    private Map<MessageSign, Message> pending = new ConcurrentHashMap<>();
    private AtomicIntegerArray next;
    private Observer observer;

    public FIFOBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                         Map<Integer, Host> idToHost, Observer observer) {
        this.pid = pid;
        this.urb = new UniformReliableBroadcast(pid, sourceIp, sourcePort, hosts, idToHost, this);
        this.observer = observer;

        int[] nextTmp = new int[hosts.size()];
        Arrays.fill(nextTmp, 1);
        this.next = new AtomicIntegerArray(nextTmp);
    }

    public void broadcast(Message message){
        urb.broadcast(new Message(pid, message.getFirstSenderId(), lsn.getAndIncrement(), message.isAck()));
    }

    // Invoked whenever the underlying urb instance delivers a message
    // Deliver the message if possible under fifo semantics
    @Override
    public void deliver(Message message) {
        int firstSender = message.getFirstSenderId();
        int seqNum = message.getSeqNum();
        // No point trying to deliver if got message with sequence number < next sequence number to be delivered
        if (seqNum >= next.get(firstSender - 1)) {
            pending.put(new MessageSign(firstSender, seqNum), message);
            Iterator<Map.Entry<MessageSign, Message>> pendingIt = pending.entrySet().iterator();
            // Check if there's a pending message with sequence number being the next expected sequence number
            // to be delivered by its first sender
            while (pendingIt.hasNext()) {
                Map.Entry<MessageSign, Message> entry = pendingIt.next();
                Message msg = entry.getValue();
                int msgFirstSender = msg.getFirstSenderId();
                // Found such messagei, increment the expected sequence number of its first sender and deliver it
                if (msg.getSeqNum() == next.get(msgFirstSender - 1)) {
                    next.incrementAndGet(msgFirstSender - 1);
                    observer.deliver(msg);
                    pendingIt.remove();
                }
            }
        }
    }
}
