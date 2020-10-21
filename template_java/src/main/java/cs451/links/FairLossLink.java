package cs451.links;

import cs451.Host;
import cs451.Message;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class FairLossLink {

    private DatagramSocket socket;

    public FairLossLink(int sourcePort, InetAddress sourceIp) {
        try {
            socket = new DatagramSocket(sourcePort, sourceIp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void send(Message message, Host host) throws IOException {
        byte[] buf = message.toData();
        DatagramPacket dpSend = new DatagramPacket(buf, buf.length, InetAddress.getByName(host.getIp()), host.getPort());
        socket.send(dpSend);
    }

    public Message receive() throws IOException {
        byte[] rec = new byte[1024];
        DatagramPacket dpReceive = new DatagramPacket(rec, rec.length);
        socket.receive(dpReceive);
        return Message.fromData(dpReceive.getData());
    }
}
