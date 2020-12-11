package cs451.broadcast;

import cs451.Host;
import cs451.Message;
import cs451.Observer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LocalCausalBroadcast implements Observer {

    // Implements Waiting Causal Broadcast algorithm with a tweak to satisfy locality constraint
    private int pid;
    private UniformReliableBroadcast urb;
    private int lsn = 0; // How many messages the process crb-broadcast
    private Set<Message> pending = ConcurrentHashMap.newKeySet();
    // vectorClock keeps track of dependencies
    // vectorClock[p-1] represents the number of messages that the process crb-delivered from process p
    // Assumes numbering of processes from 1
    private int[] vectorClock;
    private Map<Integer, Set<Integer>> causality; // Set of (pids of) processes that affect this process
    private Observer observer;
    private final ReentrantLock lock = new ReentrantLock(); // For concurrent accesses of vectorClock

    public LocalCausalBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                                Map<Integer, Host> idToHost, Map<Integer, Set<Integer>> causality, Observer observer) {
        this.pid = pid;
        this.urb = new UniformReliableBroadcast(pid, sourceIp, sourcePort, hosts, idToHost, this);
        this.vectorClock = new int[hosts.size()];
        this.causality = causality;
        this.observer = observer;
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
        pending.add(message);
        boolean tryDeliver = true;
        while (tryDeliver) {
            tryDeliver = false;
            // Iterate over all pending messages, check if it's possible to deliver any of them
            Iterator<Message> pendingIt = pending.iterator();
            while (pendingIt.hasNext()) {
                Message msg = pendingIt.next();
                int firstSender = msg.getFirstSenderId();
                boolean canDeliver = true;
                int[] msgVectorClock = msg.getVc();
                lock.lock();
                // Check if message vector clock <= our vector clock (meaning we delivered all dependencies)
                // Since dependencies aren't induced by all processes but by the ones that affect the process
                // We only check positions corresponding to processes affecting this process
                for (int p : causality.get(firstSender)) {
                    if (msgVectorClock[p-1] > vectorClock[p-1]) {
                        canDeliver = false;
                        break;
                    }
                }
                lock.unlock();
                if (canDeliver) {
                    lock.lock();
                    vectorClock[firstSender - 1]++;
                    lock.unlock();
                    observer.deliver(msg);
                    pendingIt.remove();
                    // Delivered a message, so loop again to try to deliver more
                    tryDeliver = true;
                }
            }
        }
    }
}
