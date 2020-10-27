package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Receiver extends Thread{
    private byte[] recBuffer = new byte[1024];
    private AtomicBoolean running = new AtomicBoolean(false);
    private DatagramSocket socket;
    private DeliverInterface deliverInterface;


    public Receiver(int sourcePort, DeliverInterface deliverInterface) {
        try {
            this.socket = new DatagramSocket(sourcePort);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        this.deliverInterface = deliverInterface;
    }

    @Override
    public void run() {
        running.set(true);
        while (running.get()) {
            try {
                DatagramPacket dpReceive = new DatagramPacket(recBuffer, recBuffer.length);
                socket.receive(dpReceive);
                Message message = Message.fromData(dpReceive.getData());
                System.out.println("Receiver detected message  " + message);
                deliverInterface.deliver(message);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        running.set(false);
        socket.close();
    }
}
