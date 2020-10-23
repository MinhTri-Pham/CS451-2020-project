package cs451.links;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StubbornLink {
    private int pid;
    private Map<Integer, Host> idToHost; //Mapping between pids and hosts (for ACKs)
    private FairLossLink fll; // Channel for sending and receiving
    private Set<Integer> notAcked; // Sequence numbers of sent messages not acknowledged yet
    private int timeout; // Timeout in milliseconds

    public StubbornLink(int pid, int sourcePort, InetAddress sourceIp, Map<Integer, Host> idToHost) {
        this.pid = pid;
        this.fll = new FairLossLink(sourcePort, sourceIp);
        this.notAcked = new HashSet<>();
        this.timeout = 5000;
        this.idToHost = idToHost;
    }

    public void send(Message message, Host host) throws IOException {
        int maxNotAcked = 20;
        // Can't send if already have too many unacknowledged messages
        if (notAcked.size() >= maxNotAcked) {
            System.out.println("Too many unacknowledged messages, can't send");
            return;
        }
        System.out.println("Sending message " + message);
        fll.send(message, host);
        if (!message.isAck()) {
            int seqNum = message.getSeqNum();
            notAcked.add(seqNum);
            // Retransmit if ACK not received within timeout
            // Not working for me
//            boolean acked = false;
//            while(!acked) {
//                try {
//                    System.out.println("Waiting for ACK");
//                    TimeUnit.MILLISECONDS.sleep(timeout);
//                } catch (InterruptedException ie) {
//                    Thread.currentThread().interrupt();
//                }
//                receive();
//                acked = !notAcked.contains(seqNum);
//                if (!acked) {
//                    timeout *= 2;
//                    System.out.println("Haven't received ACK, retransmit");
//                    fll.send(message, host);
//                }
//                // Message acknowledged so decrease timeout until some value - what is a good value?
//                else timeout = Math.max(timeout - 100, 250);
//            }
        }
    }

    public Message receive() throws IOException {
        Message received = fll.receive();
        int seqNum = received.getSeqNum();
        // Received data, send ACK
        if (!received.isAck()) {
            Message ackMessage = received.generateAck(pid);
            System.out.println("Send ACK for message  " + received);
            fll.send(ackMessage, idToHost.get(received.getSenderId()));
        }
        // Received ACK
        else if (notAcked.contains(seqNum)) {
            System.out.println("Received ACK for message with seqNum " + seqNum);
            notAcked.remove(seqNum);
        }
        else System.out.println("Error: Received ACK to message not sent");
        return received;
    }

    public void stop() {
        fll.stop();
    }
}
