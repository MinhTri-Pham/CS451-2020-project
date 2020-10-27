package cs451.links;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;
import cs451.Receiver;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class FairLossLink implements DeliverInterface {

    private DatagramSocket socket;
    private Receiver receiver;
    private DeliverInterface deliverInterface;

    public FairLossLink(int sourcePort, DeliverInterface deliverInterface) {
        try {
            socket = new DatagramSocket();
            this.deliverInterface = deliverInterface;
            this.receiver = new Receiver(sourcePort, this);
            receiver.start();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    public void send(Message message, Host host) {
        try {
            byte[] buf = message.toData();
            DatagramPacket dpSend = new DatagramPacket(buf, buf.length, InetAddress.getByName(host.getIp()), host.getPort());
            socket.send(dpSend);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deliver(Message message) {
        deliverInterface.deliver(message);
    }


    public void close() {
        receiver.close();
    }

}
