package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class PerfectLink implements DeliverInterface{
    private int pid;
    private DatagramSocket socket;
    // Messages sent but not acknowledged
    // The tuple stores the message sequence number and id of host from which for which ACK is expected
    private Map<Tuple<Integer, Integer>, Message> notAcked = new ConcurrentHashMap<>();
    private DeliverInterface deliverInterface;
    private Map<Integer, Host> idToHost; // Mapping between pids and hosts (for ACKs)
    private Set<Message> delivered = ConcurrentHashMap.newKeySet();
    private List<String> logs = new ArrayList<>();

    public PerfectLink(int pid, String sourceIp, int sourcePort, Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.pid = pid;
        this.idToHost = idToHost;
        try {
            this.socket = new DatagramSocket(sourcePort, InetAddress.getByName(sourceIp));
        } catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        }
        this.deliverInterface = deliverInterface;
        new Receiver().start();
        new Retransmitter().start();
    }

    private void sendUdp(Message message, Host host) {
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
            deliverInterface.deliver(message);
            logs.add(String.format("PL delivered message %s\n", message));
            System.out.println("PL delivered " + message);
        }
    }

    public void writeLog() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(String.format("pl_debug_%d.txt", pid)));
            for (String log : logs) writer.write(log);
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Thread that sends a single message via UDP
    public class Sender extends Thread {

        private Message message;
        private Host destHost;

        public Sender(Message message, Host destHost) {
            this.message = message;
            this.destHost = destHost;
        }

        @Override
        public void run() {
            if (!message.isAck()) {
                logs.add(String.format("Send %s to host %d\n", message, destHost.getId()));
                System.out.println(String.format("Send %s to host %d", message, destHost.getId()));
                notAcked.put(new Tuple<>(destHost.getId(), message.getSeqNum()), message);
                logs.add(String.format("notAcked: %s \n", notAcked.keySet()));
            }
            sendUdp(message, destHost);
        }

    }

    // Thread that handles incoming messages
    public class Receiver extends Thread {
        private byte[] recBuffer = new byte[1024];

        @Override
        public void run() {
            while(true) {
                try {
                    DatagramPacket dpReceive = new DatagramPacket(recBuffer, recBuffer.length);
                    socket.receive(dpReceive);
                    Message message = Message.fromData(dpReceive.getData());
                    if (message != null) {
                        int seqNum = message.getSeqNum();
                        int senderId = message.getSenderId();
                        // Received ACK
                        if (message.isAck()) {
                            notAcked.remove(new Tuple<>(senderId, seqNum));
                            logs.add(String.format("Received ACK, notAcked: %s \n", notAcked.keySet()));
                        }
                        // Receive DATA
                        else {
                            sendUdp(new Message(pid, message.getFirstSenderId(), seqNum, true), idToHost.get(senderId));
                            logs.add(String.format("Received %s\n", message));
                            System.out.println("Received " + message);
                            deliver(message);
                        }
                    }
                } catch (IOException e) {}
            }
        }
    }

    // Thread that periodically retransmits non-acknowledged messages
    public class Retransmitter extends Thread {
        @Override
        public void run() {
            while(true) {
                try {
                    TimeUnit.MILLISECONDS.sleep(250);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                for (Map.Entry<Tuple<Integer, Integer>, Message> pendingMsgs : notAcked.entrySet()) {
                    Message toSend = pendingMsgs.getValue();
                    int hostNum = pendingMsgs.getKey().first;
                    logs.add(String.format("Retransmit %s to host %d\n", toSend, hostNum));
                    System.out.println(String.format("Retransmit %s to host %d", toSend, hostNum));
                    sendUdp(toSend, idToHost.get(hostNum));
                }
            }
        }
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
