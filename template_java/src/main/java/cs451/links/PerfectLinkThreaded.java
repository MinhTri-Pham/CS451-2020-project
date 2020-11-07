package cs451.links;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PerfectLinkThreaded implements DeliverInterface{
    private int pid;
    private DatagramSocket socket;
    // Messages sent but not acknowledged
    // The tuple stores the message sequence number and id of host from which for which ACK is expected
    private Map<Tuple<Integer, Integer>, Message> notAcked = new ConcurrentHashMap<>();
    private DeliverInterface deliverInterface;
    private Map<Integer, Host> idToHost; // Mapping between pids and hosts (for ACKs)
    private int timeout = 1000; // Timeout in milliseconds (what is a good initial value?)
    private Set<Message> delivered = ConcurrentHashMap.newKeySet();


    public PerfectLinkThreaded(int pid, String sourceIp, int sourcePort, Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.idToHost = idToHost;
        try {
            this.socket = new DatagramSocket(sourcePort, InetAddress.getByName(sourceIp));
        } catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        }
        this.deliverInterface = deliverInterface;
        Receiver receiver = new Receiver();
        receiver.start();
    }

    public void sendUdp(Message message, Host host) {
        try {
            byte[] buf = message.toData();
            DatagramPacket dpSend = new DatagramPacket(buf, buf.length, InetAddress.getByName(host.getIp()), host.getPort());
            socket.send(dpSend);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void send(Message message, Host host) {
        new Sender(message, host).start();
    }

    @Override
    public void deliver(Message message) {
        if (!delivered.contains(message)) {
            delivered.add(message);
            System.out.println("BEB delivered " + message);
            deliverInterface.deliver(message);
        }
        else {
            System.out.println("Don't BEB deliver duplicate " + message);
            System.out.println(delivered);
        }
    }

    // Thread to message under PL semantics
    public class Sender extends Thread {

        private Message message;
        private Host destHost;

        public Sender(Message message, Host destHost) {
            this.message = message;
            this.destHost = destHost;
        }

        @Override
        public void run() {
            int maxNotAcked = 2;
            if (!message.isAck()) {
                while (notAcked.size() >= maxNotAcked) {
                    System.out.println("Too many unacknowledged messages, might have to resend");
                    try {
                        TimeUnit.MILLISECONDS.sleep(timeout);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }

                    // If not, resend unacknowledged messages
                    // Double timeout for next waiting
                    if (notAcked.size() >= maxNotAcked) {
                        for (Map.Entry<Tuple<Integer, Integer>, Message> pendingMsgs : notAcked.entrySet()) {
                            System.out.println("Resend " + pendingMsgs.getValue() + " to host " + pendingMsgs.getKey().first);
                            sendUdp(pendingMsgs.getValue(), idToHost.get(pendingMsgs.getKey().first));
                        }
                        timeout *= 2;
                    }
                    // By how much to decrease?
                    else timeout = Math.max(timeout - 100, 250);
                }
                notAcked.put(new Tuple<>(destHost.getId(), message.getSeqNum()), message);
            }
            sendUdp(message, destHost);
        }

    }

    // Handle incoming messages
    public class Receiver extends Thread {
        private byte[] recBuffer = new byte[1024];
        private AtomicBoolean running = new AtomicBoolean(false);

        @Override
        public void run() {
            running.set(true);
            while (running.get()) {
                try {
                    DatagramPacket dpReceive = new DatagramPacket(recBuffer, recBuffer.length);
                    socket.receive(dpReceive);
                    Message message = Message.fromData(dpReceive.getData());
                    if (message != null) {
                        int seqNum = message.getSeqNum();
                        int senderId = message.getSenderId();
                        // Received ACK
                        if (message.isAck()) {
                            System.out.println("Received ACK message " + message);
                            notAcked.remove(new Tuple<>(senderId, seqNum));
                        }
                        // Receive DATA
                        else {
                            System.out.println("Received DATA message " + message);
                            Message ackMessage = new Message(pid, message.getFirstSenderId(), message.getSeqNum(), true);
                            System.out.println(String.format("Sending ACK message %s to host %d", ackMessage, message.getSenderId()));
                            sendUdp(ackMessage, idToHost.get(message.getSenderId()));
                            deliver(message);
                        }
                    }
                } catch (IOException e) {}
            }
        }

//        public void close() {
//            running.set(false);
//            socket.close();
//        }
    }

    // Helper class to track acknowledged messages
    public static class Tuple<X, Y> {
        public final X first;
        public final Y second;

        public Tuple(X first, Y second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tuple<?, ?> tuple = (Tuple<?, ?>) o;
            return Objects.equals(first, tuple.first) &&
                    Objects.equals(second, tuple.second);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }

        @Override
        public String toString() {
            return "(" +
                    first + ", " +
                    second + ')';
        }
    }
}
