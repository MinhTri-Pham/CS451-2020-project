package cs451.links;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StubbornLink implements DeliverInterface {
    private int pid; // Pid of process
    private Map<Integer, Host> idToHost; // Mapping between pids and hosts (for ACKs)
    private FairLossLink fll; // Channel for sending and receiving
    // Messages sent but not acknowledged
    // The tuple stores the process id and the message sequence number from resp. for which ACK is expected
    private Set<Tuple<Integer, Integer>> notAcked = ConcurrentHashMap.newKeySet();
    private DeliverInterface deliverInterface;
    private int timeout = 250; // Timeout in milliseconds

    public StubbornLink(int pid, int sourcePort, Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.fll = new FairLossLink(sourcePort, this);
        this.idToHost = idToHost;
        this.deliverInterface = deliverInterface;
    }

    // TO DO: Test this really works when there are network delays, processes are down
    public void send(Message message, Host host){
        if (message.isAck()) {
            // ACKs be sent immediately
            System.out.println(String.format("Sending ACK message %s to host %d", message, host.getId()));
            fll.send(message, host);
        }
        else {
            // For DATA messages, have to make some checks
            //Wait if we have too many unacknowledged messages
//            while (notAcked.size() >= 20) {
//                System.out.println("Too many unacknowledged messages, can't send");
//                try {
//                    TimeUnit.MILLISECONDS.sleep(1000);
//                } catch (InterruptedException ie) {
//                    Thread.currentThread().interrupt();
//                }
//
//            }
            System.out.println(String.format("Sending DATA message %s to host %d", message, host.getId()));
            fll.send(message, host);
            Tuple<Integer, Integer> toAck = new Tuple<>(host.getId(), message.getSeqNum());
            notAcked.add(toAck);
            System.out.println("Added  " + toAck + " to nonAcked");
            // Retransmit if ACK not received within timeout
//            while(notAcked.contains(toAck)) {
//                System.out.println("Waiting for ACK");
//                try {
//                    TimeUnit.MILLISECONDS.sleep(timeout);
//                } catch (InterruptedException ie) {
//                    Thread.currentThread().interrupt();
//                }
//                if (notAcked.contains(toAck)) {
//                    timeout *= 2;
//                    System.out.println("Haven't received ACK, double timeout and retransmit");
//                    fll.send(message, host);
//                }
//                // Message acknowledged so decrease timeout until some value - increase by what?
//                else timeout = Math.max(timeout - 100, 250);
//            }
        }
    }

    @Override
    public void deliver(Message message) {
        int seqNum = message.getSeqNum();
        int senderId = message.getSenderId();

        // Received ACK
        if (message.isAck()) {
            System.out.println("Received ACK message " + message);
            Tuple<Integer, Integer> acked = new Tuple<>(senderId, seqNum);
            notAcked.remove(acked);
            System.out.println("Removed  " + acked + " from nonAcked");
        }
        else {
            System.out.println("Received DATA message " + message);
            Message ackMessage = message.generateAck(pid);
            System.out.println(String.format("Sending ACK message %s to host %d", ackMessage, message.getSenderId()));
            fll.send(ackMessage, idToHost.get(message.getSenderId()));
            deliverInterface.deliver(message);
        }
    }

    public void close() {
        fll.close();
    }


    // Helper class to track acknowledged messages
    public static class Tuple<X, Y> {
        public final X first;
        public final Y second;

        public Tuple(X first, Y second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple<?, ?> tuple = (Tuple<?, ?>) o;
            return Objects.equals(first, tuple.first) &&
                    Objects.equals(second, tuple.second);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }

        @Override
        public String toString() {
            return "(" +
                    first + ", " +
                    second + ')';
        }
    }
}
