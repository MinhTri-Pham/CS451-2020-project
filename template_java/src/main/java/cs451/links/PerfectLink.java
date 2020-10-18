package cs451.links;

import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

public class PerfectLink {

    private StubbornLink sl;
    private Set<Message> delivered;

    public PerfectLink(int pid, int sourcePort, InetAddress sourceIp) {
        sl = new StubbornLink(pid, sourcePort, sourceIp);
        delivered = new HashSet<>();
    }

    public void send(Message message) throws IOException {
        sl.send(message);
    }

    public Message receive() throws IOException {
        Message received = sl.receive();
        if (!delivered.contains(received)) {
            delivered.add(received);
            return received;
        }
        else {
            System.out.println("Already received message");
            return null;
        }
    }

}
