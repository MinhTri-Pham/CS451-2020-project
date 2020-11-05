package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class Sender implements Runnable {

    private DatagramSocket socket;
    private Message message;
    private InetAddress ip;
    private int destPort;

    public Sender(DatagramSocket socket, Message message, InetAddress ip, int destPort) {
        this.socket = socket;
        this.message = message;
        this.ip = ip;
        this.destPort = destPort;
    }

    @Override
    public void run() {
        try {
            byte[] buf = message.toData();
            DatagramPacket dpSend = new DatagramPacket(buf, buf.length, ip, destPort);
            socket.send(dpSend);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
