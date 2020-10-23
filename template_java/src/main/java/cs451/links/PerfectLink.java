package cs451.links;

import cs451.Host;
import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PerfectLink {

    private StubbornLink sl; // Underlying channel
    private Set<Integer> extra; // Set of sequence numbers bigger than maxContiguous + 1  of received messages
    private int maxContiguous; // Maximum sequence number n such that all messages 1,...,n have been received

    public PerfectLink(int pid, int sourcePort, InetAddress sourceIp, Map<Integer, Host> idToHost) {
        sl = new StubbornLink(pid, sourcePort, sourceIp, idToHost);
        extra = new HashSet<>();
        maxContiguous = 0;
    }

    public void send(Message message, Host host) throws IOException {
        sl.send(message,host);
    }

    public Message receive() throws IOException {
        Message received = sl.receive();
        int receivedSeqNum = received.getSeqNum();
        // Check if we have to update data structures for received messages
        if (receivedSeqNum > maxContiguous && !extra.contains(receivedSeqNum)) {
            if (receivedSeqNum == maxContiguous + 1) {
                // Received contiguous message
                int i = 1;
                // Check if sequence numbers in extra together with received sequence number form a contiguous sequence
                while (extra.contains(receivedSeqNum + i)) {
                    extra.remove(receivedSeqNum + i);
                }
                // Minus 1 because the while loop above terminates when it finds first non-contiguous number
                maxContiguous = receivedSeqNum + i - 1;
            }
            // Non contiguous message
            else extra.add(receivedSeqNum);
            return received;
        }
        else {
            System.out.println("Already received this");
            return null;
        }
    }

    public void stop() {
        sl.stop();
    }

}
