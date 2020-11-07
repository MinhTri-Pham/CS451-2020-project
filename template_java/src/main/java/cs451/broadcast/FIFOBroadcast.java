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

    public FIFOBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                         Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.urb = new UniformReliableBroadcast(pid, sourceIp, sourcePort, hosts, idToHost, this);
        this.deliverInterface = deliverInterface;

        int[] nextTmp = new int[hosts.size()];
        Arrays.fill(nextTmp, 1);
        this.next = new AtomicIntegerArray(nextTmp);
    }

    public void broadcast(Message message){
        urb.broadcast(new Message(pid, message.getFirstSenderId(), lsn.getAndIncrement(), message.isAck()));
    }

    @Override
    public void deliver(Message message) {
        int firstSender = message.getFirstSenderId();
        int seqNum = message.getSeqNum();
        // No point trying to deliver if got message with sequence number < next sequence to be delivered
        // process with id firstSender
        if (seqNum >= next.get(firstSender - 1)) {
            pending.put(new MessageSign(firstSender, seqNum), message);
            Iterator<Map.Entry<MessageSign, Message>> pendingIt = pending.entrySet().iterator();
            while (pendingIt.hasNext()) {
                Map.Entry<MessageSign, Message> entry = pendingIt.next();
                Message msg = entry.getValue();
                int msgFirstSender = msg.getFirstSenderId();
                if (msg.getSeqNum() == next.get(msgFirstSender - 1)) {
                    next.incrementAndGet(msgFirstSender - 1);
                    deliverInterface.deliver(msg);
                    pendingIt.remove();
                }
            }
        }
    }

//    public void close() {
//        urb.close();
//    }
}
