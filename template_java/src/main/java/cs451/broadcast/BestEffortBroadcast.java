package cs451.broadcast;

import cs451.Message;
import cs451.links.*;
import cs451.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

public class BestEffortBroadcast {

    private PerfectLink pl;
    private List<Host> hosts;

    public BestEffortBroadcast(int sourcePort, InetAddress sourceIp,List<Host> hosts) {
        this.hosts = hosts;
        pl = new PerfectLink(sourcePort, sourceIp);
    }

    public void broadcast(Message message) throws IOException {
        for (Host host : hosts) {
            pl.send(message, host.getPort(), InetAddress.getByName(host.getIp()));
        }
    }

    public Message receive() throws IOException {
        return pl.receive();
    }
}
