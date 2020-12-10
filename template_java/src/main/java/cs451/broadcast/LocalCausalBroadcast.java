package cs451.broadcast;

import cs451.Host;
import cs451.Message;
import cs451.Observer;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class LocalCausalBroadcast implements Observer {

    // Implements Waiting Causal Broadcast algorithm with a tweak to satisfy locality constraint
    private int pid;
    private UniformReliableBroadcast urb;
    private int lsn = 0; // How many messages the process crb-broadcast
    private Set<Message> pending = new HashSet<>();
    // vectorClock keeps track of dependencies
    // vectorClock[p-1] represents the number of messages that the process crb-delivered from process p
    // Assumes numbering of processes from 1
    private int[] vectorClock;
    private Map<Integer, Set<Integer>> causality; // Set of (pids of) processes that affect this process
    private Observer observer;
    private final ReentrantLock lock = new ReentrantLock(); // For concurrent accesses of pending and vectorClock

    public LocalCausalBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                                Map<Integer, Host> idToHost, Map<Integer, Set<Integer>> causality, Observer observer) {
        this.pid = pid;
        this.urb = new UniformReliableBroadcast(pid, sourceIp, sourcePort, hosts, idToHost, this);
        this.vectorClock = new int[hosts.size()];
        this.causality = causality;
        this.observer = observer;
    }

    // Determine if a vector clock W <= a vector clock V
    // Since dependencies aren't induced by all processes but by the ones that affect the process
    // We only check positions corresponding to processes affecting this process
    public boolean compareVectorClocks(int[] W, int[] V, Set<Integer> dependencies) {
        for (int p : dependencies) {
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
        boolean tryDeliver = true;
        while (tryDeliver) {
            tryDeliver = false;
            // Iterate over all pending messages, check if it's possible to deliver any of them
            Iterator<Message> pendingIt = pending.iterator();
            while (pendingIt.hasNext()) {
                Message msg = pendingIt.next();
                int firstSender = msg.getFirstSenderId();
                // Check if we delivered all dependencies using the vector clock
                if (compareVectorClocks(msg.getVc(), vectorClock, causality.get(firstSender))) {
                    vectorClock[firstSender - 1]++;
                    observer.deliver(msg);
                    pendingIt.remove();
                    // Delivered a message, so loop again to try to deliver more
                    tryDeliver = true;
                }
            }
        }
        lock.unlock();
    }
}
