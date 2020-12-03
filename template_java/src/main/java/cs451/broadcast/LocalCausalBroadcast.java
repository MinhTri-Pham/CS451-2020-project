package cs451.broadcast;

import cs451.Host;
import cs451.Message;
import cs451.MessageSign;
import cs451.Observer;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class LocalCausalBroadcast implements Observer {

    // Implements Waiting Causal Broadcast algorithm with a tweak to satisfy locality constraint
    // Most importantly, we use two vector clocks instead of just one
    // V_send: number of dependencies for a newly sent message
    // V_recv: number of messages delivered by other processes
    private int pid;
    private int hostNum;
    private UniformReliableBroadcast urb;
    private int lsn = 0;
    private Map<Integer, Set<Message>> pending = new HashMap<>();
    private int[] V_send;
    private int[] V_recv;
    private Set<Integer> causality; // Set of (pids of) processes that affect this process
    private Observer observer;
    // Two separate locks for controlling the two vector clocks
    private final ReentrantLock lock_send = new ReentrantLock();
    private final ReentrantLock lock_recv = new ReentrantLock();

    public LocalCausalBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                                Map<Integer, Host> idToHost, Set<Integer> causality, Observer observer) {
        this.pid = pid;
        this.hostNum = hosts.size();
        this.urb = new UniformReliableBroadcast(pid, sourceIp, sourcePort, hosts, idToHost, this);
        this.V_send = new int[hostNum];
        this.V_recv = new int[hostNum];
        for (int i = 0; i < hostNum; i++) {
            pending.put(i, new HashSet<>());
        }
        this.causality = causality;
        this.observer = observer;
    }

    // Return if vector clock W <= vector clock V (assume they have the same length (hostnum))
    // This means W[i] <= V[i] for all i in 0, ..., hostnum-1
    public boolean compareVClocks(int[] W, int[] V) {
        for (int i = 0; i < hostNum; i++) {
            if (W[i] > V[i]) return false;
        }
        return true;
    }

    public void broadcast(Message message) {
        lock_send.lock();
        // With each broadcast message, we send its dependencies (V_send)
        int[] W = V_send.clone();
        W[pid-1] = lsn;
        lsn++;
        urb.broadcast(new Message(pid, message.getFirstSenderId(), message.getSeqNum(), message.isAck(), W));
        lock_send.unlock();
    }

    // Invoked whenever the underlying urb instance delivers a message
    // Deliver the message if possible under crb semantics
    @Override
    public void deliver(Message message) {
        lock_recv.lock();
        pending.computeIfAbsent(message.getFirstSenderId() - 1, sender-> new HashSet<>()).add(message);
        boolean deliverAgain = true;
        while (deliverAgain) {
            deliverAgain = false;
            // Iterate over all processes, check if it's possible to deliver any pending message sent by the process
            for (int p = 0; p < hostNum; p++) {
                Iterator<Message> pendingIt = pending.get(p).iterator();
                Message msg = pendingIt.next();
                while (pendingIt.hasNext()) {
                    // Deliver only if we already delivered all dependencies of an urb delivered message
                    // I.e. W (msg.getVc()) <= V_recv
                    if (compareVClocks(msg.getVc(), V_recv)) {
                        V_recv[p]++;
                        // Messages sent in the future will depend on this message only if the message was sent
                        // by a process affecting this process
                        // Hence why V_send incremented only if first sender of message affects this process
                        if (causality.contains(p+1)) {
                            lock_send.lock();
                            V_send[p]++;
                            lock_recv.unlock();
                        }
                        observer.deliver(msg);
                        pendingIt.remove();
                        // Delivered one message, we loop again to deliver more
                        deliverAgain = true;
                    }
                }
            }
        }
        lock_recv.unlock();
    }
}
