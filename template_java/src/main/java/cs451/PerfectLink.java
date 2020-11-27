package cs451;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

// Channel for reliably sending messages
public class PerfectLink{
    private int pid;
    private DatagramSocket socket;
    // notAcked stores messages sent but not acknowledged (a set of messages per each host to which we sent a message)
    private Map<Integer, Set<Message>> notAcked = new ConcurrentHashMap<>();
    private Observer observer;
    private Map<Integer, Host> idToHost; // Mapping between process ids and hosts (for ACKs)
    private Set<Message> delivered = ConcurrentHashMap.newKeySet();

    public PerfectLink(int pid, String sourceIp, int sourcePort, Map<Integer, Host> idToHost, Observer observer) {
        this.pid = pid;
        this.idToHost = idToHost;
        try {
            this.socket = new DatagramSocket(sourcePort, InetAddress.getByName(sourceIp));
        } catch (SocketException | UnknownHostException e) {
            e.printStackTrace();
        }
        this.observer = observer;
        new Receiver().start();
        new Retransmitter().start();
    }

    // Sends a message to a specified host
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
                // Indicate that we're waiting for an ACK for this message
                int destId = destHost.getId();
                notAcked.computeIfAbsent(destId, absentId -> ConcurrentHashMap.newKeySet());
                notAcked.get(destId).add(message);
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
                            notAcked.get(senderId).remove(message);
                        }
                        // Received DATA - send an ACK and deliver the message if it's unique
                        else {
                            sendUdp(new Message(pid, message.getFirstSenderId(), seqNum, true, message.getVc()), idToHost.get(senderId));
                            if (!delivered.contains(message)) {
                                delivered.add(message);
                                observer.deliver(message);
                            }
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
                    TimeUnit.MILLISECONDS.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                for (Map.Entry<Integer, Set<Message>> pendingMsgs : notAcked.entrySet()) {
                    int hostNum = pendingMsgs.getKey();
                    Set<Message> toSend = pendingMsgs.getValue();
                    for (Message msg : toSend) {
                        sendUdp(msg, idToHost.get(hostNum));
                    }
                }
            }
        }
    }
}
