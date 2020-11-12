package cs451.broadcast;

import cs451.Observer;
import cs451.Message;
import cs451.PerfectLink;
import cs451.Host;

import java.util.List;
import java.util.Map;

public class BestEffortBroadcast implements Observer {

    private PerfectLink pl;
    private List<Host> hosts;
    // Observer to whom we deliver the message
    // We actually pass an URB instance (so beb-delivering triggers urb-delivering mechanism)
    private Observer observer;

    public BestEffortBroadcast(int pid, String sourceIp, int sourcePort, List<Host> hosts,
                               Map<Integer, Host> idToHost, Observer observer) {
        this.hosts = hosts;
        this.observer = observer;
        this.pl = new PerfectLink(pid, sourceIp, sourcePort, idToHost, this);
    }

    // Sends a message to all hosts via a Perfect Link channel
    public void broadcast(Message message) {
        for (Host host : hosts) {
            pl.send(message, host);
        }
    }

    @Override
    public void deliver(Message message) {
        observer.deliver(message);
    }
}
