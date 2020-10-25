package cs451.broadcast;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class FIFOBroadcast implements DeliverInterface{
    private UniformReliableBroadcast urb;
    private AtomicInteger lsn = new AtomicInteger(0);
    private Set<Message> pending = ConcurrentHashMap.newKeySet();;
    private AtomicIntegerArray next;
    private DeliverInterface deliverInterface;

    public FIFOBroadcast(int pid, int sourcePort, List<Host> hosts,
                         Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        // Implements Broadcast with Sequence Number algorithm
        this.urb = new UniformReliableBroadcast(pid, sourcePort, hosts, idToHost, this);
        this.deliverInterface = deliverInterface;
        this.next = new AtomicIntegerArray(hosts.size());
    }

    public void broadcast(Message message){
        urb.broadcast(message.withSeqNum(lsn.incrementAndGet()));
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

//    public Message deliver() throws IOException {
//        Message urbDelivered = urb.deliver();
//        if (urbDelivered == null) return null;
//        pending.add(urbDelivered);
//        for (Message pendingMsg : pending) {
//            if (next[pendingMsg.getSenderId()] == pendingMsg.getSeqNum()) {
//                next[pendingMsg.getSenderId()]++;
//                pending.remove(pendingMsg);
//                // Don't think this return is right
//                // Specification implies we should deliver all such messages
//                return pendingMsg;
//            }
//        }
//        // Don't need pending contains check?
//        if (pending.contains(urbDelivered) &&  next[urbDelivered.getSenderId()] == urbDelivered.getSeqNum()) {
//            next[urbDelivered.getSenderId()]++;
//            pending.remove(urbDelivered);
//            return urbDelivered;
//        }
//        // Not sure about this
//        return null;
//    }

    public void close() {
        urb.close();
    }
}
