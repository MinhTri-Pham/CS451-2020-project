package cs451.links;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PerfectLink implements DeliverInterface {

    private StubbornLink sl; // Underlying channel
    Set<Message> delivered = ConcurrentHashMap.newKeySet();
    // To compress representation of delivered set,
    // for each sender, store sequence number sn such that messages with sequence number 1,..., sn have been delivered
    Map<Integer, Integer> maxContiguous = new ConcurrentHashMap<>();
    private DeliverInterface deliverInterface;

    public PerfectLink(int pid, int sourcePort, Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.sl = new StubbornLink(pid, sourcePort, idToHost, this);
        this.deliverInterface = deliverInterface;
    }

    public void send(Message message, Host host) {
        sl.send(message,host);
    }

    @Override
    public void deliver(Message message) {
        int msgSenderId = message.getSenderId();
        int msgSeqNum = message.getSeqNum();

        // First message
        if (maxContiguous.get(msgSenderId) == null && msgSeqNum == 1) {
            maxContiguous.put(msgSenderId, 1);
            delivered.add(message);
            System.out.println(maxContiguous);
            System.out.println(delivered);
            deliverInterface.deliver(message);
        }
        // Received a duplicate message
        else if (delivered.contains(message) || msgSeqNum <= maxContiguous.get(msgSenderId)) {
            System.out.println("Received duplicate message " + message);
        }
        else {
            // Received a contiguous message, update maxContiguous and delivered
            if (msgSeqNum == maxContiguous.get(msgSenderId) + 1) {
                int i = 1;
                Message temp = new Message(msgSenderId, msgSeqNum + i, false);
                // Check if we have a new a contiguous sequence
                while (delivered.contains(temp)) {
                    delivered.remove(temp);
                    i++;
                    temp = new Message(msgSenderId, msgSeqNum + i, false);
                }
                // No +1 because the while loop above terminates when it finds first non-contiguous number
                maxContiguous.put(msgSenderId, msgSeqNum + i - 1);
            }
            // Received a non-contiguous message, update delivered
            else {
                delivered.add(message);
            }
            // Deliver the message (regardless of whether it's contiguous or not)
            System.out.println(maxContiguous);
            System.out.println(delivered);
            deliverInterface.deliver(message);
        }
    }

    public void close () {
        sl.close();
    }

}
