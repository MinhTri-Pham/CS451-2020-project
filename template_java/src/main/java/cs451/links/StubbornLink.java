package cs451.links;

import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;


public class StubbornLink {
    private FairLossLink fllSend; // Channel for sending data
    private FairLossLink fllRec; // Channel for receiving data/ACKs
    private Set<Integer> notAcked; // Messages not acknowledged yet

    public StubbornLink(int sourcePort, InetAddress sourceIp) {
        this.fllSend = new FairLossLink();
        this.fllRec = new FairLossLink(sourcePort, sourceIp);
        this.notAcked = new HashSet<>();
    }

    public void send(Message message, int destPort, InetAddress destIp) throws IOException {
        System.out.println("Sending message " + message);
        fllSend.send(message, destPort, destIp);
        notAcked.add(message.getSeqNum());
    }


    public Message receive() throws IOException {
        Message received = fllRec.receive();
        System.out.println("Received message " + received);
        int seqNum = received.getSeqNum();
        // Received data, send ACK
        if (!received.isAck() && notAcked.contains(seqNum)) {
            System.out.println("Sending ACK to " + received);
            Message ackMessage = received.generateAck();
            fllSend.send(ackMessage, received.getSourcePort(), received.getSourceIp());
            notAcked.remove(seqNum);
        }
        return received;
    }
}
