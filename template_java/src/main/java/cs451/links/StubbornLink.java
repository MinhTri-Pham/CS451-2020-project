package cs451.links;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StubbornLink implements DeliverInterface {
    private int pid;
    private Map<Integer, Host> idToHost; //Mapping between pids and hosts (for ACKs)
    private FairLossLink fll; // Channel for sending and receiving
    private Set<Integer> notAcked = ConcurrentHashMap.newKeySet(); // Sequence numbers of sent messages not acknowledged yet
    private DeliverInterface deliverInterface;
    private int timeout = 1000; // Timeout in milliseconds -2

    public StubbornLink(int pid, int sourcePort, Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.fll = new FairLossLink(sourcePort, this);
        this.idToHost = idToHost;
        this.deliverInterface = deliverInterface;
    }

    public void send(Message message, Host host){
        if (!message.isAck()) {
            // ACKs be sent immediately
            System.out.println(String.format("Sending message %s to host %d", message, host.getId()));
            fll.send(message, host);
        }
        else {
            // For DATA messages, have to make some checks
            // What to set this to? Change it throughout execution?
            int maxNotAcked = 20;
            // Wait if we have too many unacknowledged messages
            while (notAcked.size() >= maxNotAcked) {
                System.out.println("Too many unacknowledged messages, can't send");
            }
            System.out.println(String.format("Sending message %s to host %d", message, host.getId()));
            fll.send(message, host);
            int seqNum = message.getSeqNum();
            notAcked.add(seqNum);
            // Retransmit if ACK not received within timeout
//            while(notAcked.contains(seqNum)) {
//                System.out.println("Waiting for ACK");
//                try {
//                    TimeUnit.MILLISECONDS.sleep(timeout);
//                } catch (InterruptedException ie) {
//                    Thread.currentThread().interrupt();
//                }
//                if (notAcked.contains(seqNum)) {
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
        // Received DATA, send ACK
        if (!message.isAck()) {
            Message ackMessage = message.generateAck(pid);
            System.out.println(String.format("Sending ACK message %s to host %d", ackMessage, message.getSenderId()));
            fll.send(ackMessage, idToHost.get(message.getSenderId()));
            System.out.println("SL deliver message " + message);
            deliverInterface.deliver(message);
        }
        // Received ACK
        else if (notAcked.contains(seqNum)) {
            System.out.println("Received ACK for message with seqNum " + seqNum);
            notAcked.remove(seqNum);
        }
        else System.out.println("Error: Received ACK to message not sent");
    }

    public void close() {
        fll.close();
    }
}
