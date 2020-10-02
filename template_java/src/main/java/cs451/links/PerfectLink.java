package cs451.links;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PerfectLink {

    private StubbornLink sl;
    private List<String> delivered;

    public PerfectLink() {
        sl = new StubbornLink();
        delivered = new ArrayList<>();
    }

    public void send(String message, String destIp, int destPort) throws IOException {
        sl.send(message,destIp,destPort);
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
