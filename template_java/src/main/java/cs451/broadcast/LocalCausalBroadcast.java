package cs451.broadcast;

import cs451.Host;
import cs451.Message;
import cs451.MessageSign;
import cs451.Observer;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class LocalCausalBroadcast implements Observer {

    // Implements Waiting Causal Broadcast algorithm with locality constraint
    private int pid;
    private int hostNum;
    private UniformReliableBroadcast urb;
    private int lsn = 1;
    private Map<MessageSign, Message> pending = new HashMap<>();
    private int[] V_send;
    private int[] V_recv;
    private Set<Integer> causality;
    private Observer observer;

    private final ReentrantLock lock = new ReentrantLock();


    public LocalCausalBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                                Map<Integer, Host> idToHost, Set<Integer> causality, Observer observer) {
        this.pid = pid;
        this.hostNum = hosts.size();
        this.urb = new UniformReliableBroadcast(pid, sourceIp, sourcePort, hosts, idToHost, this);
        this.V_send = new int[hostNum];
        this.V_recv = new int[hostNum];
        Arrays.fill(V_send,1);
        Arrays.fill(V_recv,1);
        this.causality = causality;
        this.observer = observer;
    }

    // Return if vector clock W <= vector clock V
    public boolean compareVClocks(int[] W, int[] V) {
        for (int i = 0; i < hostNum; i++) {
            if (W[i] > V[i]) return false;
        }
        return true;
    }

    public void broadcast(Message message) {
        lock.lock();
        int[] W = V_send.clone();
        W[pid-1] = lsn;
        urb.broadcast(new Message(pid, message.getFirstSenderId(), lsn, message.isAck(), W));
        lsn++;
        lock.unlock();
    }

    @Override
    public void deliver(Message message) {
        int firstSender = message.getFirstSenderId();
        int seqNum = message.getSeqNum();
        lock.lock();
        pending.put(new MessageSign(firstSender, seqNum), message);
        Iterator<Map.Entry<MessageSign, Message>> pendingIt = pending.entrySet().iterator();
        while (pendingIt.hasNext()) {
            Map.Entry<MessageSign, Message> entry = pendingIt.next();
            Message msg = entry.getValue();
            int msgFirstSender = msg.getFirstSenderId();
            if (compareVClocks(msg.getVc(), V_recv)) {
                V_recv[msgFirstSender-1]++;
                if (causality.contains(msgFirstSender)) {
                    V_send[msgFirstSender-1]++;
                    observer.deliver(msg);
                    pendingIt.remove();
                }

            }
        }
        lock.unlock();
    }
}
