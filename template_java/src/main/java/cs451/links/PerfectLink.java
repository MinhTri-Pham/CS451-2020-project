package cs451.links;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PerfectLink {

    private StubbornLink sl;
    private List<String> delivered;

    public PerfectLink(int sourcePort, String sourceIp) {
        sl = new StubbornLink(sourcePort, sourceIp);
        delivered = new ArrayList<>();
    }

    public void send(String message, int destPort, String destIp) throws IOException {
        sl.send(message,destPort,destIp);
    }

    public String receive() throws IOException {
        String received = sl.receive();
        if (!delivered.contains(received)) {
            delivered.add(received);
            return received;
        }
        else {
            System.out.println("Already received message");
            return null;
        }
    }

}
