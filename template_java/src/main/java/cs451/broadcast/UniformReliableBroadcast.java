package cs451.broadcast;

import cs451.DeliverInterface;
import cs451.Host;
import cs451.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class UniformReliableBroadcast implements DeliverInterface {

    // Implements Majority ACK algorithm
    private BestEffortBroadcast beb;
    Set<Message> delivered = ConcurrentHashMap.newKeySet();
    // To compress representation of delivered set,
    // for each sender, store sequence number sn such that messages with sequence number 1,..., sn have been delivered
    Map<Integer, Integer> maxContiguous = new ConcurrentHashMap<>();
    private Set<Message> pending = ConcurrentHashMap.newKeySet();
    private ConcurrentHashMap<Message, Set<Integer>> ack = new ConcurrentHashMap<>();
    private List<Host> hosts;
    private DeliverInterface deliverInterface;

    public UniformReliableBroadcast(int pid, int sourcePort, List<Host> hosts,
                                    Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.beb = new BestEffortBroadcast(pid, sourcePort, hosts, idToHost, this);
        this.hosts = hosts;
        this.deliverInterface = deliverInterface;
    }

    private boolean canDeliver(Message m) {
        return 2*ack.get(m).size() > hosts.size();
    }

    public void broadcast(Message m) {
        pending.add(m);
        beb.broadcast(m);
    }

    @ Override
    public void deliver(Message message) {
        // Implements upon event <beb, Deliver ...>
        int messageSenderId = message.getSenderId();
        Set<Integer> bebDeliveredAck = ack.get(message);
        if (bebDeliveredAck == null) {
            Set<Integer> singleSet = ConcurrentHashMap.newKeySet();
            singleSet.add(messageSenderId);
            ack.put(message, singleSet);
        }
        else {
            bebDeliveredAck.add(messageSenderId);
            ack.put(message, bebDeliveredAck);
        }

        if (!pending.contains(message)) {
            pending.add(message);
            beb.broadcast(message);
        }

        // Implements upon exists (s,m) in pending such that candeliver(m) ∧ (m not in delivered)
        for (Message pendingMsg : pending) {
            int pendingMsgSeqNum = pendingMsg.getSeqNum();
            int pendingMsgSenderId = pendingMsg.getSenderId();
            if (canDeliver(pendingMsg) && maxContiguous.get(pendingMsgSenderId) != null
                    && pendingMsgSeqNum > maxContiguous.get(pendingMsgSenderId) && !delivered.contains(pendingMsg)) {
                // Add pendingMsg to delivered set
                if (pendingMsgSeqNum == maxContiguous.get(pendingMsg.getSenderId() + 1)) {
                    // Contiguous message
                    int i = 1;
                    Message temp = new Message(pendingMsgSenderId, pendingMsgSeqNum + i, false);
                    // Check if we have a new a contiguous sequence
                    while (delivered.contains(temp)) {
                        delivered.remove(temp);
                        i++;
                        temp = new Message(pendingMsgSenderId, pendingMsgSeqNum + i, false);
                    }
                    // No +1 because the while loop above terminates when it finds first non-contiguous number
                    maxContiguous.put(pendingMsgSenderId, pendingMsgSeqNum + i - 1);
                }
                // Non-contiguous message
                else delivered.add(pendingMsg);
                deliverInterface.deliver(pendingMsg);
                pending.remove(pendingMsg); // Garbage clean pending
            }
        }
    }

    public void close() {
        beb.close();
    }
}
