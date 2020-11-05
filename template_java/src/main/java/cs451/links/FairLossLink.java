package cs451.links;

import cs451.*;

import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class FairLossLink implements DeliverInterface {
    private static final int NTHREADS = Runtime.getRuntime().availableProcessors();
    private ExecutorService executor = Executors.newFixedThreadPool(NTHREADS);
    private DatagramSocket[] sockets;
    private Receiver receiver;
    private DeliverInterface deliverInterface;

    public FairLossLink(int sourcePort, DeliverInterface deliverInterface) {
        try {
            this.sockets = new DatagramSocket[NTHREADS];
            for (int i = 0; i < NTHREADS; i++) {
                sockets[i] = new DatagramSocket();
            }
            this.deliverInterface = deliverInterface;
            this.receiver = new Receiver(sourcePort, this);
            receiver.start();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    public void send(Message message, Host host) {
//        try {
//            byte[] buf = message.toData();
//            DatagramPacket dpSend = new DatagramPacket(buf, buf.length, InetAddress.getByName(host.getIp()), host.getPort());
//            socket.send(dpSend);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        // Choose random socket and run new worker to send
        try {
            int random = ThreadLocalRandom.current().nextInt(0, NTHREADS);
            executor.execute(new Sender(sockets[random], message, InetAddress.getByName(host.getIp()), host.getPort()));
        } catch (UnknownHostException e) {
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
