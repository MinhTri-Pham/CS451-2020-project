package cs451;

import cs451.broadcast.BestEffortBroadcast;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Process implements DeliverInterface {
    private int pid;
    private int nbMessagesToBroadcast;
    private BestEffortBroadcast bestEffortBroadcast;
    private List<String> logs; // Store logs in memory while broadcasting/delivering
    private String output; // Name of output file

    public Process(int pid, int port, List<Host> hosts,
                   int nbMessagesToBroadcast, String output) {
        this.pid = pid;
        this.nbMessagesToBroadcast = nbMessagesToBroadcast;
        this.logs = new ArrayList<>();
        this.output = output;

        Map<Integer, Host> idToHost = new HashMap<>();
        for (Host host : hosts) idToHost.put(host.getId(), host);
        this.bestEffortBroadcast = new BestEffortBroadcast(pid, port, hosts, idToHost, this);
    }

    public void broadcast() {
        for (int i = 1; i <= nbMessagesToBroadcast; i++) {
            Message broadcastMsg = new Message(pid, i, false);
            bestEffortBroadcast.broadcast(broadcastMsg);
            logs.add(String.format("b %d\n",i));
        }
    }

    @Override
    public void deliver(Message message) {
        logs.add(String.format("d %d %d\n", message.getSenderId(), message.getSeqNum()));
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

    public void close() {
        bestEffortBroadcast.close();
    }
}