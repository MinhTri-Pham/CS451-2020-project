package cs451.broadcast;

import cs451.DeliverInterface;
import cs451.Message;
import cs451.PerfectLink;
import cs451.Host;

import java.util.List;
import java.util.Map;

public class BestEffortBroadcast implements DeliverInterface {

    private PerfectLink pl;
    private List<Host> hosts;
    private DeliverInterface deliverInterface;

    public BestEffortBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                               Map<Integer, Host> idToHost, DeliverInterface deliverInterface) {
        this.hosts = hosts;
        this.deliverInterface = deliverInterface;
        this.pl = new PerfectLink(pid, sourceIp, sourcePort, idToHost, this);
    }

    public void broadcast(Message message) {
        for (Host host : hosts) {
            pl.send(message, host);
        }
    }

    @Override
    public void deliver(Message message) {
        deliverInterface.deliver(message);
    }

    public void close() {
        pl.writeLog();
    }
}
