package cs451;

import cs451.broadcast.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

// Represents a process that broadcasts and delivers messages, logging these events in an output file
public class Process implements Observer {
    private int pid;
    private int nbMessagesToBroadcast;
    private LocalCausalBroadcast lcb;
    private List<String> logs = new ArrayList<>();
    private String output; // Name of output file
    private int hostNum;

    public Process(int pid, String ip, int port, List<Host> hosts,
                   int nbMessagesToBroadcast, Map<Integer, Set<Integer>> causality, String output) {
        this.pid = pid;
        this.nbMessagesToBroadcast = nbMessagesToBroadcast;
        this.output = output;
        this.hostNum = hosts.size();
        // Make mapping from process ids to hosts
        Map<Integer, Host> idToHost = new HashMap<>();
        for (Host host : hosts) idToHost.put(host.getId(), host);
        this.lcb = new LocalCausalBroadcast(pid, ip, port, hosts, idToHost, causality,this);
    }

    // Broadcast all required messages and log
    public void broadcast() {
        int[] dummyVClock = new int[hostNum];
        for (int i = 1; i <= nbMessagesToBroadcast; i++) {
            Message broadcastMsg = new Message(pid, pid, i,false, dummyVClock);
            lcb.broadcast(broadcastMsg);
            logs.add(String.format("b %d\n",i));
        }
    }

    // Invoked whenever the underlying crb instance delivers a message
    // Log this event
    @Override
    public void deliver(Message message) {
        logs.add(String.format("d %d %d\n", message.getFirstSenderId(), message.getSeqNum()));
    }

    public void writeLog() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(output));
            for (String log : logs) writer.write(log);
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
