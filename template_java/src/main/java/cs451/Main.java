package cs451;

import cs451.links.FairLossLink;
import cs451.links.PerfectLink;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

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
            testPerfectLinkTwoHosts(parser);
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
        if (parser.myId() == 1) {
            FairLossLink fl1 = new FairLossLink(h1.getPort(), h1.getIp());
            System.out.println("Sent Hi process 2");
            fl1.send("Hi process 2",h2.getPort(), h2.getIp());
            String received = fl1.receive();
            if (received != null) System.out.println("Received " + received);
        }

        else {
            FairLossLink fl2 = new FairLossLink(h2.getPort(), h2.getIp());
            System.out.println("Sent Hi process 1");
            fl2.send("Hi process 1",h1.getPort(), h1.getIp());
            String received = fl2.receive();
            if (received != null) System.out.println("Received " + received);
        }
    }

    private static void testPerfectLinkTwoHosts(Parser parser) throws IOException {
        System.out.println("Test perfect link");
        Host h1 = parser.hosts().get(0);
        Host h2 = parser.hosts().get(1);
        if (parser.myId() == 1) {
            PerfectLink pf1 = new PerfectLink(h1.getPort(), h1.getIp());
            System.out.println("Sent Hi process 2");
            pf1.send("Hi process 2",h2.getPort(), h2.getIp());
            String received = pf1.receive();
            if (received != null) System.out.println("Received " + received);
        }

        else {
            PerfectLink pf2 = new PerfectLink(h2.getPort(), h2.getIp());
            System.out.println("Sent Hi process 1");
            pf2.send("Hi process 1",h1.getPort(), h1.getIp());
            String received = pf2.receive();
            if (received != null) System.out.println("Received " + received);
        }
    }
}
