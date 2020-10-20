package cs451.links;

import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class StubbornLink {
    private int pid;
    private FairLossLink fll; // Channel for sending and receiving
    private Set<Integer> notAcked; // Sequence numbers of sent messages not acknowledged yet
    private int timeout; // Timeout in milliseconds

    public StubbornLink(int pid, int sourcePort, InetAddress sourceIp) {
        this.pid = pid;
        this.fll = new FairLossLink(sourcePort, sourceIp);
        this.notAcked = new HashSet<>();
        this.timeout = 250;
    }

    public void send(Message message, int destPort, InetAddress destIp) throws IOException {
        int maxNotAcked = 20;
        // Can't send if already have too many unacknowledged messages
        if (notAcked.size() >= maxNotAcked) {
            System.out.println("Too many unacknowledged messages, can't send");
            return;
        }
        System.out.println("Sending message " + message);
        fll.send(message, destPort, destIp);
        int seqNum = message.getSeqNum();
        notAcked.add(seqNum);
        boolean acked = false;
        // Retransmit if ACK not received within timeout
        if (!message.isAck()) {
            while(!acked) {
                try {
                    System.out.println("Waiting for ACK");
                    TimeUnit.MILLISECONDS.sleep(timeout);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                receive();
                acked = !notAcked.contains(seqNum);
                if (!acked) {
                    timeout *= 2;
                    System.out.println("Haven't received ACK, retransmit");
                    fll.send(message, destPort, destIp);
                }
                // Message acknowledged so decrease timeout - what is a good value?
                else timeout -= 100;
            }
        }
    }

    public Message receive() throws IOException {
        Message received = fll.receive();
        System.out.println("Received message " + received);
        int seqNum = received.getSeqNum();
        // Received data, send ACK
        if (!received.isAck()) {
            Message ackMessage = received.generateAck(pid);
            System.out.println("Send ACK for message with seqNum " + seqNum);
            fll.send(ackMessage, received.getSourcePort(), received.getSourceIp());
        }
        // Received ACK
        else if (notAcked.contains(seqNum)) {
            System.out.println("Received ACK for message with seqNum " + seqNum);
            notAcked.remove(seqNum);
        }
        else System.out.println("Error: Received ACK to message not sent");
        return received;
    }
}
