package cs451.broadcast;

import cs451.Host;
import cs451.Message;
import cs451.Observer;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class LocalCausalBroadcast implements Observer {

    // Implements Waiting Causal Broadcast algorithm with a tweak to satisfy locality constraint
    private int pid;
    private int hostNum;
    private UniformReliableBroadcast urb;
    private int lsn = 0;
    private Set<Message> pending = new HashSet<>();
    private int[] vectorClock;
    private Map<Integer, Set<Integer>> causality; // Set of (pids of) processes that affect this process
    private Observer observer;
    private final ReentrantLock lock = new ReentrantLock();

    public LocalCausalBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                                Map<Integer, Host> idToHost, Map<Integer, Set<Integer>> causality, Observer observer) {
        this.pid = pid;
        this.hostNum = hosts.size();
        this.urb = new UniformReliableBroadcast(pid, sourceIp, sourcePort, hosts, idToHost, this);
        this.vectorClock = new int[hostNum];
        this.causality = causality;
        this.observer = observer;
    }

    public boolean compareVectorClocks(int[] W, int[] V, Set<Integer> dependecies) {
        for (int p : dependecies) {
            if (W[p-1] > V[p-1]) return false;
        }
        return true;
    }

    public void broadcast(Message message) {
        lock.lock();
        // With each broadcast message, we send its dependencies
        int[] W = vectorClock.clone();
        W[pid-1] = lsn;
        lsn++;
        lock.unlock();
        urb.broadcast(new Message(pid, message.getFirstSenderId(), message.getSeqNum(), message.isAck(), W));
    }

    // Invoked whenever the underlying urb instance delivers a message
    // Deliver the message if possible under lcb semantics
    @Override
    public void deliver(Message message) {
        lock.lock();
        pending.add(message);
        boolean deliverAgain = true;
        while (deliverAgain) {
            deliverAgain = false;
            // Iterate over all pending messages, check if it's possible to deliver any of them
            Iterator<Message> pendingIt = pending.iterator();
            while (pendingIt.hasNext()) {
                Message msg = pendingIt.next();
                int firstSender = msg.getFirstSenderId();
                if (compareVectorClocks(msg.getVc(), vectorClock, causality.get(firstSender))) {
                    vectorClock[firstSender - 1]++;
                    observer.deliver(msg);
                    pendingIt.remove();
                    // Delivered one message, we loop again to try to deliver more
                    deliverAgain = true;
                }
            }
        }
        lock.unlock();
    }
}
