package cs451;

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

        System.out.println("Test perfect link");
        try {
            testPerfectLink(parser);
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

    private static void testPerfectLink(Parser parser) throws IOException {
        if (parser.myId() == 1) {
            PerfectLink pf1 = new PerfectLink();
            Host dest = parser.hosts().get(1);
            for (int i = 0; i < 5; i++) {
                pf1.send(Integer.toString(i), dest.getIp(), dest.getPort());
                String received = pf1.receive();
                if (received != null) System.out.println(received);
            }
        }

        else {
            PerfectLink pf2 = new PerfectLink();
            Host dest = parser.hosts().get(0);
            for (int i = 5; i < 10; i++) {
                pf2.send(Integer.toString(i), dest.getIp(), dest.getPort());
                String received = pf2.receive();
                if (received != null) System.out.println(received);
            }
        }

    }
}
