package cs451.broadcast;

import cs451.links.*;
import cs451.Host;

import java.io.IOException;
import java.util.List;

public class BestEffortBroadcast {

    private PerfectLink pl;
    private List<Host> hosts;

    public BestEffortBroadcast(List<Host> hosts) {
        this.hosts = hosts;
        pl = new PerfectLink();
    }

    public void broadcast(String message) throws IOException {
        for (Host host : hosts) {
            pl.send(message, host.getIp(), host.getId());
        }
    }

    public String receive() throws IOException {
        return pl.receive();
    }
}
