package cs451.links;

import java.io.IOException;

public class StubbornLink {
    private FairLossLink fll;

    public StubbornLink(int sourcePort, String sourceIp) {
        this.fll = new FairLossLink(sourcePort, sourceIp);
    }

    public void send(String message, int destPort, String destIp) throws IOException {
        while(true)
            fll.send(message, destPort, destIp);
    }

    public String receive() throws IOException {
        return fll.receive();
    }
}
