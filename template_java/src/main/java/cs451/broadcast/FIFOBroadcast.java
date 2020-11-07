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
        System.out.println("next: " + next);
        System.out.println(String.format("next[%d] = %d", firstSender, next.get(firstSender)));
        // No point trying to deliver if got message with sequence number < next sequence to be delivered
        // process with id firstSender
        if (seqNum >= next.get(firstSender)) {
            pending.put(new MessageSign(firstSender, seqNum), message);
            System.out.println(pending);
            Iterator<Map.Entry<MessageSign, Message>> pendingIt = pending.entrySet().iterator();
            while (pendingIt.hasNext()) {
                Map.Entry<MessageSign, Message> entry = pendingIt.next();
                Message msg = entry.getValue();
                int msgFirstSender = msg.getFirstSenderId();
                if (msg.getSeqNum() == next.get(msgFirstSender)) {
                    System.out.println(String.format("Found msg %s with seq num %d = next[%d]", msg, msg.getSeqNum(), next.get(msgFirstSender)));
                    next.incrementAndGet(msgFirstSender);
                    deliverInterface.deliver(msg);
                    System.out.println("FIFO deliver: " + msg);
                    pendingIt.remove();
                }
            }
        }
    }

//    public void close() {
//        urb.close();
//    }
}
