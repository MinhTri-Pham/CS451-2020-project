package cs451.broadcast;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class UniformReliableBroadcast {

    // Implements Majority ACK algorithm
    private Integer pid; // Pid of broadcaster (represents self in the algorithm)
    private BestEffortBroadcast beb;
    private Set<Message> delivered;
    private Set<Pair<Integer, Message>> pending;
    private Map<Message, Set<Integer>> ack;
    private List<Host> hosts;

    public UniformReliableBroadcast(int pid, int sourcePort, InetAddress sourceIp, List<Host> hosts) {
        this.pid = pid;
        this.beb = new BestEffortBroadcast(pid, sourcePort, sourceIp, hosts);
        this.delivered = new HashSet<>();
        this.pending =  new HashSet<>();
        this.ack = new HashMap<>();
        this.hosts = hosts;
    }

    private boolean canDeliver(Message m) {
        return 2*ack.get(m).size() > hosts.size();
    }

    public void broadcast(Message m) throws IOException {
        pending.add(new Pair<>(pid, m));
        beb.broadcast(m);
    }

    public void deliver(Message m) throws IOException {
//        Message received = beb.receive();
//        if (received == null) return;
        Set<Integer> mAck = ack.get(m);
        mAck.add(pid);
        ack.put(m, mAck);
        Pair<Integer, Message> pair = new Pair<>(m.getSenderId(), m);
        if (!pending.contains(pair)) {
            pending.add(pair);
            beb.broadcast(m);
        }
        for (Pair<Integer, Message> pendingPair : pending) {
            Message pendingMsg = pendingPair.second;
            if (canDeliver(pendingMsg) && !delivered.contains(pendingMsg))
                delivered.add(pendingMsg);
        }
    }

    private static class Pair<A, B> {
        private A first;
        private B second;

        private Pair(A first, B second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair<?, ?> pair = (Pair<?, ?>) o;
            return Objects.equals(first, pair.first) &&
                    Objects.equals(second, pair.second);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }
    }

}
