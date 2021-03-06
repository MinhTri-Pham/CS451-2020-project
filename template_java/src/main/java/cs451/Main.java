package cs451;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    private static Process myProcess;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
        myProcess.writeLog();
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

        int nbMessagesToBroadcast = 0;

        // if config is defined; always check before parser.config()
        List<String> configLines = null;
        if (parser.hasConfig()) {
            String configPath = parser.config();
            System.out.println("Config: " + configPath);
            // Find number of messages to broadcast
            try {
                configLines = Files.readAllLines(Paths.get(configPath));
                nbMessagesToBroadcast =  configLines.size() > 1 ? Integer.parseInt(configLines.get(0)) : 0;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Map<Integer, Set<Integer>> causality = new HashMap<>();
        // Causal relations
        assert configLines != null;
        if (configLines.size() > 1) {
            for (int i = 1; i < configLines.size(); i++) {
                String causalityLine = configLines.get(i);
                String[] causalityProcesses = causalityLine.split(" ");
                int process = Integer.parseInt(causalityProcesses[0]);
                Set<Integer> dependencies = new HashSet<>();
                for (String causalityProcess : causalityProcesses) {
                    dependencies.add(Integer.parseInt(causalityProcess));
                }
                causality.put(process, dependencies);
            }
        }

        // Find my info among hosts and initialise new Process
        int myId = parser.myId();
        for (Host host: parser.hosts()) {
            if (host.getId() == parser.myId()) {
                myProcess = new Process(myId, host.getIp(), host.getPort(),
                        parser.hosts(), nbMessagesToBroadcast, causality, parser.output());
                break;
            }
        }

        Coordinator coordinator = new Coordinator(parser.myId(), parser.barrierIp(), parser.barrierPort(), parser.signalIp(), parser.signalPort());

	    System.out.println("Waiting for all processes for finish initialization");
        coordinator.waitOnBarrier();

	    System.out.println("Broadcasting messages...");
        myProcess.broadcast();

	    System.out.println("Signaling end of broadcasting messages");
        coordinator.finishedBroadcasting();

	    while (true) {
	        // Sleep for 1 hour
	        Thread.sleep(60 * 60 * 1000);
	    }
    }


}
