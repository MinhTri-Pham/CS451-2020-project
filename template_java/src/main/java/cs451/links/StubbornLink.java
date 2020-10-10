package cs451.links;

import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;

public class StubbornLink {
    private FairLossLink fllSend; // Channel for sending data
    private FairLossLink fllRec; // Channel for receiving data/ACKs

    public StubbornLink(int sourcePort, InetAddress sourceIp) {
        this.fllSend = new FairLossLink();
        this.fllRec = new FairLossLink(sourcePort, sourceIp);
    }

    public void send(Message message, int destPort, InetAddress destIp) throws IOException {
        boolean receivedAck = false;
        while (!receivedAck) {
            System.out.println("Sending message " + message);
            fllSend.send(message, destPort, destIp);
            Message received = fllRec.receive();
            System.out.println("Received message " + received);
            if (received.getSeqNum() == message.getSeqNum() && received.isAck()) {
                System.out.println("Got acknowledgment for sent message");
                receivedAck = true;
            }
            else System.out.println("Error");
        }
    }


    public Message receive() throws IOException {
        Message received = fllRec.receive();
        System.out.println("Received message " + received);
        // Received data, send ACK
        if (!received.isAck()) {
            System.out.println("Sending ACK");
            Message ackMessage = received.generateAck();
            fllSend.send(ackMessage, received.getSourcePort(), received.getSourceIp());
        }
        return received;
    }
}
