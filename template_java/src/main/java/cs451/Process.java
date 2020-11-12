package cs451;

import cs451.broadcast.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Represents a process that broadcasts and delivers messages, logging these events in an output file
public class Process implements Observer {
    private int pid;
    private int nbMessagesToBroadcast;
    private FIFOBroadcast fifo;
    private List<String> logs = new ArrayList<>(); //
    private String output; // Name of output file

    public Process(int pid, String ip, int port, List<Host> hosts,
                   int nbMessagesToBroadcast, String output) {
        this.pid = pid;
        this.nbMessagesToBroadcast = nbMessagesToBroadcast;
        this.output = output;
        // Make mapping from process ids to hosts
        Map<Integer, Host> idToHost = new HashMap<>();
        for (Host host : hosts) idToHost.put(host.getId(), host);
        this.fifo = new FIFOBroadcast(pid, ip, port, hosts, idToHost, this);
    }

    // Broadcast all required messages and log
    public void broadcast() {
        for (int i = 1; i <= nbMessagesToBroadcast; i++) {
            Message broadcastMsg = new Message(pid, pid, i,false);
            fifo.broadcast(broadcastMsg);
            logs.add(String.format("b %d\n",i));
        }
    }

    // Invoked whenever the underlying fifo instance delivers a message
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
