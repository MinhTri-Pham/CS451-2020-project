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
        notAcked.add(message.getSeqNum());
        // Stop-and-go protocol
        try {
            TimeUnit.MILLISECONDS.sleep(250);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        while(notAcked.contains(message.getSeqNum())) {
            fll.send(message, destPort, destIp);
        }
    }

    public Message receive() throws IOException {
        Message received = fll.receive();
        System.out.println("Received message " + received);
        int seqNum = received.getSeqNum();
        // Received data, send ACK
        if (!received.isAck()) {
            Message ackMessage = received.generateAck(pid);
            System.out.println("Send ACK message " + ackMessage);
            fll.send(ackMessage, received.getSourcePort(), received.getSourceIp());
            notAcked.remove(seqNum);
        }
        // Received ACK
        else if (notAcked.contains(seqNum)) notAcked.remove(seqNum);
        else System.out.println("Error: Received ACK to message not sent");
        return received;
    }
}
