package cs451.links;

import cs451.Message;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

public class PerfectLink {

    private StubbornLink sl;
    private Set<Integer> extra;
    private int maxContiguous;

    public PerfectLink(int pid, int sourcePort, InetAddress sourceIp) {
        sl = new StubbornLink(pid, sourcePort, sourceIp);
        extra = new HashSet<>();
        maxContiguous = 0;
    }

    public void send(Message message, int destPort, InetAddress destIp) throws IOException {
        sl.send(message,destPort,destIp);
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

}
