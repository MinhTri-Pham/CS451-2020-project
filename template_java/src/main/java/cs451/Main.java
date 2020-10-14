package cs451;

import cs451.links.FairLossLink;
import cs451.links.PerfectLink;
import cs451.links.StubbornLink;

import java.io.IOException;
import java.net.InetAddress;

public class Main {

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID is " + pid + ".");
        System.out.println("Use 'kill -SIGINT " + pid + " ' or 'kill -SIGTERM " + pid + " ' to stop processing packets.");

        System.out.println("My id is " + parser.myId() + ".");
        System.out.println("List of hosts is:");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId() + ", " + host.getIp() + ", " + host.getPort());
        }

        System.out.println("Barrier: " + parser.barrierIp() + ":" + parser.barrierPort());
        System.out.println("Signal: " + parser.signalIp() + ":" + parser.signalPort());
        System.out.println("Output: " + parser.output());
        // if config is defined; always check before parser.config()
        if (parser.hasConfig()) {
            System.out.println("Config: " + parser.config());
        }


        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());

	    System.out.println("Waiting for all processes for finish initialization");
        coordinator.waitOnBarrier();

//	    System.out.println("Broadcasting messages...");

        try {
//            testFairLossLinkTwoHosts(parser);
            testStubbornLink(parser);
        } catch (Exception e) {
            e.printStackTrace();
        }


	    System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();

	    while (true) {
	        // Sleep for 1 hour
	        Thread.sleep(60 * 60 * 1000);
	    }
    }

    private static void testFairLossLinkTwoHosts(Parser parser) throws IOException {
        System.out.println("Test fair-loss link");
        Host h1 = parser.hosts().get(0);
        Host h2 = parser.hosts().get(1);
        int h1Port = h1.getPort();
        InetAddress h1Ip = InetAddress.getByName(h1.getIp());
        int h2Port = h2.getPort();
        InetAddress h2Ip = InetAddress.getByName(h2.getIp());
        if (parser.myId() == 1) {
            FairLossLink fl1 = new FairLossLink(h1Port, h1Ip);
            Message m1 = new Message(1, h1Port, h1Ip, h2Port, h2Ip);
            System.out.println("Sending " + m1 + " to host 2");
            fl1.send(m1,h2Port, h2Ip);
            Message received = fl1.receive();
            if (received != null) System.out.println("Received " + received);
        }

        else {
            FairLossLink fl2 = new FairLossLink(h2Port, h2Ip);
            Message m2 = new Message(2, h2Port, h2Ip, h1Port, h1Ip);
            System.out.println("Sending " + m2 + " to host 1");
            fl2.send(m2,h1Port, h1Ip);
            Message received = fl2.receive();
            if (received != null) System.out.println("Received " + received);
        }
    }

    private static void testStubbornLink(Parser parser) throws IOException {
        System.out.println("Test (optimised) stubborn link");
        Host h1 = parser.hosts().get(0);
        Host h2 = parser.hosts().get(1);
        int h1Port = h1.getPort();
        InetAddress h1Ip = InetAddress.getByName(h1.getIp());
        int h2Port = h2.getPort();
        InetAddress h2Ip = InetAddress.getByName(h2.getIp());
        if (parser.myId() == 1) {
            StubbornLink sl1 = new StubbornLink(h1Port, h1Ip);
//            Message m1 = new Message(1, h1Port, h1Ip, h2Port, h2Ip);
//            sl1.send(m1,h2Port, h2Ip);
//            sl1.receive();
//            sl1.receive();

            for (int i = 1; i <= 5; i++) {
                Message m = new Message(i, h1Port, h1Ip, h2Port, h2Ip);
                sl1.send(m,h2Port, h2Ip);
                sl1.receive();
                sl1.receive();
            }
        }

        else {
            StubbornLink sl2 = new StubbornLink(h2Port, h2Ip);
//            Message m2 = new Message(2,h2Port, h2Ip, h1Port, h1Ip);
//            sl2.send(m2,h1Port, h1Ip);
//            sl2.receive();
//            sl2.receive();

            for (int i = 6; i <= 10; i++) {
                Message m = new Message(i, h1Port, h1Ip, h2Port, h2Ip);
                sl2.send(m,h1Port, h1Ip);
                sl2.receive();
                sl2.receive();
            }
        }
    }
}
