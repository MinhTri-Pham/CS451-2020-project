package cs451.links;

import cs451.Message;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class FairLossLink {

    private DatagramSocket socket;

    public FairLossLink() {
        try {
            socket = new DatagramSocket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public FairLossLink(int sourcePort, String sourceIp) {
        try {
            socket = new DatagramSocket(sourcePort, InetAddress.getByName(sourceIp));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void send(Message message, int destPort, InetAddress destIp) throws IOException {
        byte[] buf = message.toData();
        DatagramPacket dpSend = new DatagramPacket(buf, buf.length, destIp, destPort);
        socket.send(dpSend);
    }

    public Message receive() throws IOException {
        byte[] rec = new byte[1024];
        DatagramPacket dpReceive = new DatagramPacket(rec, rec.length);
        socket.receive(dpReceive);
        return Message.fromData(dpReceive.getData());
    }

    public InetAddress getIP() {
        return socket.getInetAddress();
    }

    public int getPort() {
        return socket.getPort();
    }

}
