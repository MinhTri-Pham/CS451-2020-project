package cs451.links;

import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;


public class StubbornLink {
//    private FairLossLink fllSend; // Channel for sending data
//    private FairLossLink fllRec; // Channel for receiving data/ACKs
    private FairLossLink fll; // Channel for receiving data/ACKs
    private Set<Integer> notAcked; // Messages not acknowledged yet

    public StubbornLink(int sourcePort, InetAddress sourceIp) {
//        this.fllSend = new FairLossLink();
//        this.fllRec = new FairLossLink(sourcePort, sourceIp);
        this.fll = new FairLossLink(sourcePort, sourceIp);
        this.notAcked = new HashSet<>();
    }

    public void send(Message message, int destPort, InetAddress destIp) throws IOException {
        System.out.println("Sending message " + message);
//        fllSend.send(message, destPort, destIp);
        fll.send(message, destPort, destIp);
        notAcked.add(message.getSeqNum());
    }


    public Message receive() throws IOException {
//        Message received = fllRec.receive();
        Message received = fll.receive();
        System.out.println("Received message " + received);
        int seqNum = received.getSeqNum();
        // Received data, send ACK
        if (!received.isAck()) {
            System.out.println("Sending ACK to " + received);
            Message ackMessage = received.generateAck();
//            fllSend.send(ackMessage, received.getSourcePort(), received.getSourceIp());
            fll.send(ackMessage, received.getSourcePort(), received.getSourceIp());
            notAcked.remove(seqNum);
        }
        // Received ACK
        else if (notAcked.contains(seqNum)) notAcked.remove(seqNum);
        else System.out.println("Error: Received ACK to message not sent");
        return received;
    }
}
