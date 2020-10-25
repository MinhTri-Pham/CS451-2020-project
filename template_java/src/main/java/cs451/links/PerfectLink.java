package cs451.links;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PerfectLink implements DeliverInterface {

    private StubbornLink sl; // Underlying channel
    // Compressed representation of delivered set
    private Set<Integer> extra = ConcurrentHashMap.newKeySet(); // Set of sequence numbers bigger than maxContiguous + 1  of received messages
    // Maximum sequence number n such that all messages 1,...,n have been received
    private int maxContiguous = 0; // Maximum sequence number n such that all messages 1,...,n have been received
    private DeliverInterface deliverInterface;

    public PerfectLink(int pid, int sourcePort, Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.sl = new StubbornLink(pid, sourcePort, idToHost, this);
        this.deliverInterface = deliverInterface;
    }

    public void send(Message message, Host host) {
        sl.send(message,host);
    }

    //TO DO: concurrent access of global variables
    @Override
    public void deliver(Message message) {
        int receivedSeqNum = message.getSeqNum();

        // Check if we have to update data structures for received messages
        if (receivedSeqNum > maxContiguous && !extra.contains(receivedSeqNum)) {
            if (receivedSeqNum == maxContiguous + 1) {
                // Received contiguous message
                int i = 1;
                // Check if we have a new a contiguous sequence
                while (extra.contains(receivedSeqNum + i)) {
                    extra.remove(receivedSeqNum + i);
                }
                // Minus 1 because the while loop above terminates when it finds first non-contiguous number
                maxContiguous = receivedSeqNum + i - 1;
            }
            // Non contiguous message
            else extra.add(receivedSeqNum);
            deliverInterface.deliver(message);
        }
        else {
            System.out.println("Already received this");
        }
    }

    //    public Message receive() throws IOException {
//        Message received = sl.receive();
//        int receivedSeqNum = received.getSeqNum();
//        // Check if we have to update data structures for received messages
//        if (receivedSeqNum > maxContiguous && !extra.contains(receivedSeqNum)) {
//            if (receivedSeqNum == maxContiguous + 1) {
//                // Received contiguous message
//                int i = 1;
//                // Check if sequence numbers in extra together with received sequence number form a contiguous sequence
//                while (extra.contains(receivedSeqNum + i)) {
//                    extra.remove(receivedSeqNum + i);
//                }
//                // Minus 1 because the while loop above terminates when it finds first non-contiguous number
//                maxContiguous = receivedSeqNum + i - 1;
//            }
//            // Non contiguous message
//            else extra.add(receivedSeqNum);
//            return received;
//        }
//        else {
//            System.out.println("Already received this");
//            return null;
//        }
//    }

    public void close () {
        sl.close();
    }

}
