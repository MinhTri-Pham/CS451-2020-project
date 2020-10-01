package cs451.links;

import java.io.IOException;

public class StubbornLink {
    private FairLossLink fll;

    public StubbornLink() {
        this.fll = new FairLossLink();
    }

    public void send(String message, String destIp, int destPort) throws IOException {
        while(true)
            fll.send(message, destIp, destPort);
    }

    public String receive() throws IOException {
        return fll.receive();
    }
}
