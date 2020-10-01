package cs451.links;

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

    public void send(String message, String destIp, int destPort) throws IOException {
        InetAddress ip = InetAddress.getByName(destIp);
        byte[] buf = message.getBytes();
        DatagramPacket dpSend = new DatagramPacket(buf, buf.length, ip, destPort);
        socket.send(dpSend);
    }

    public String receive() throws IOException {
        byte[] rec = new byte[1024];
        DatagramPacket dpReceive = new DatagramPacket(rec, rec.length);
        socket.receive(dpReceive);
        return new String(dpReceive.getData(),0, dpReceive.getLength());
    }

}