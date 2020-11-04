package cs451.links;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StubbornLink implements DeliverInterface {
    private int pid; // Pid of process
    private Map<Integer, Host> idToHost; // Mapping between pids and hosts (for ACKs)
    private FairLossLink fll; // Channel for sending and receiving
    // Messages sent but not acknowledged
    // The tuple stores the message sequence number and id of host from which for which ACK is expected
    private Map<Tuple<Integer, Integer>, Message> notAcked = new ConcurrentHashMap<>();
    private DeliverInterface deliverInterface;
    private int timeout = 250; // Timeout in milliseconds (what is a good initial value)

    public StubbornLink(int pid, int sourcePort, Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.fll = new FairLossLink(sourcePort, this);
        this.idToHost = idToHost;
        this.deliverInterface = deliverInterface;
    }

    public void send(Message message, Host host){
        // ACKs can be sent immediately
        // For DATA messages, enforce some flow control
        if (!message.isAck()) {
            // If too many unacknowledged messages, have to wait for acknowledgements (can't send new messages)
            // What is a good value?
            int maxNotAcked = 20;
            while (notAcked.size() >= maxNotAcked) {
                // Wait for some time to see if acknowledgements arrive
                try {
                    TimeUnit.MILLISECONDS.sleep(timeout);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                // If not, resend unacknowledged messages
                // Double timeout for next waiting
                if (notAcked.size() >= maxNotAcked) {
                    for (Map.Entry<Tuple<Integer, Integer>, Message> pendingMsgs : notAcked.entrySet()) {
                        fll.send(pendingMsgs.getValue(), idToHost.get(pendingMsgs.getKey().first));
                    }
                    timeout *= 2;
                }
                // By how much to decrease?
                else timeout = Math.max(timeout - 100, 250);
            }
            notAcked.put(new Tuple<>(host.getId(), message.getSeqNum()), message);
            System.out.println("notAcked " + notAcked);
        }
        fll.send(message, host);
    }

    @Override
    public void deliver(Message message) {
        int seqNum = message.getSeqNum();
        int senderId = message.getSenderId();

        // Received ACK
        if (message.isAck()) {
            System.out.println("Received ACK message " + message);
            notAcked.remove(new Tuple<>(senderId, seqNum));
            System.out.println("notAcked " + notAcked);
        }
        else {
            System.out.println("Received DATA message " + message);
            Message ackMessage = new Message(pid, message.getFirstSenderId(), message.getSeqNum(), true);
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
