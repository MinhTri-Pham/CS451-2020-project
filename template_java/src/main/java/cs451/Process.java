package cs451;

import cs451.broadcast.BestEffortBroadcast;
import cs451.broadcast.FIFOBroadcast;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Process {
    private int pid;
    private int nbMessagesToBroadcast;
    private BestEffortBroadcast bestEffortBroadcast;
    private List<String> logs; // Store logs in memory while broadcasting/delivering
    private String output; // Name of output file
    private boolean delivering = false;

    public Process(int pid, int port, String ip, List<Host> hosts, int nbMessagesToBroadcast, String output) {
        this.pid = pid;
        this.nbMessagesToBroadcast = nbMessagesToBroadcast;
        this.output = output;
        this.logs = new ArrayList<>();

        Map<Integer, Host> idToHost = new HashMap<>();
        for (Host host : hosts) idToHost.put(host.getId(), host);
        try {
            this.bestEffortBroadcast = new BestEffortBroadcast(pid, port, InetAddress.getByName(ip), hosts, idToHost);
            }
        catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void broadcast() {
        startDelivering();
        for (int i = 1; i <= nbMessagesToBroadcast; i++) {
            Message broadcastMsg = new Message(pid, i, false);
            try {
                bestEffortBroadcast.broadcast(broadcastMsg);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            logs.add(String.format("b %d\n",i));
        }
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

    public void startDelivering() {
        delivering = true;
        Message delivered = null;
        while(delivering) {
            try {
                delivered = bestEffortBroadcast.deliver();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            // Log non-ACK messages
            if (delivered != null && !delivered.isAck())
                logs.add(String.format("d %d %d \n", delivered.getSenderId(), delivered.getSeqNum()));
        }
    }

    public void stopDelivering() {
        delivering = false;
        bestEffortBroadcast.stop();
    }
}
