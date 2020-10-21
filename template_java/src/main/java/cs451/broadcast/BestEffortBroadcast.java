package cs451.broadcast;

import cs451.Message;
import cs451.links.*;
import cs451.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public class BestEffortBroadcast {

    private PerfectLink pl;
    private List<Host> hosts;

    public BestEffortBroadcast(int pid, int sourcePort, InetAddress sourceIp,List<Host> hosts, Map<Integer, Host> idToHost) {
        this.hosts = hosts;
        pl = new PerfectLink(pid, sourcePort, sourceIp, idToHost);
    }

    public void broadcast(Message message) throws IOException {
        for (Host host : hosts) {
            pl.send(message, host);
        }
    }

    public Message deliver() throws IOException {
        return pl.receive();
    }
}
