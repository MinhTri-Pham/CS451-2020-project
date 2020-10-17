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
    private Set<Integer> notAcked; // Messages not acknowledged yet

    public StubbornLink(int pid, int sourcePort, InetAddress sourceIp) {
        this.pid = pid;
        this.fll = new FairLossLink(sourcePort, sourceIp);
        this.notAcked = new HashSet<>();
    }

    public void send(Message message, int destPort, InetAddress destIp) throws IOException {
        System.out.println("Sending message " + message);
        fll.send(message, destPort, destIp);
        int seqNum = message.getSeqNum();
        notAcked.add(seqNum);
        boolean acked = false;
        // Retransmit if ACK not received within some time
        if (!message.isAck()) {
            while(!acked) {
                try {
                    System.out.println("Waiting for ACK");
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                Message rec = receive();
                System.out.println(rec);
                acked = notAcked.contains(seqNum);
                if (!acked) {
                    System.out.println("Haven't received ACK, retransmit");
                    fll.send(message, destPort, destIp);
                }
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
