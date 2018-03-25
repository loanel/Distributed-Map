package mat.distmap;

import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;

import java.net.InetAddress;
import java.util.Scanner;

public class Runner {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.net.preferIPv4Stack", "true");
        String clusterName = "mateusz-home";
        String udpIp = "\"230.0.0.2\"";
        JChannel channel = initializeChannel(udpIp);
        DistributedMap mapInstance = new DistributedMap(channel, clusterName);
        clientRoutine(mapInstance);
    }


    private static void clientRoutine(DistributedMap map) {
        Scanner reader = new Scanner(System.in);
        System.out.println("Entering client loop");
        while (reader.hasNextLine()) {
            String[] message = reader.nextLine().split("\\s+");
            parseMessage(message, map);
        }
    }

    private static void parseMessage(String[] message, DistributedMap map) {
        String command = message[0];
        switch (command) {
            case "put":
                if (message.length == 3) {
                    map.put(message[1], message[2]);
                } else {
                    System.out.println("Wrong amount of arguments, required format for put: put [key] [value]");
                }
                break;
            case "remove":
                map.remove(message[1]);
                break;
            case "display":
                map.displayDataset();
                break;
            case "contains":
                System.out.println(map.containsKey(message[1]));
                break;
            default:
                System.out.println("Wrong command, available commands: put, remove, display, contains");
        }
    }

    private static JChannel initializeChannel(String udpIp) throws Exception {
        JChannel channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName(udpIp)))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL()
                        .setValue("timeout", 12000)
                        .setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE_TRANSFER())
                .addProtocol(new SEQUENCER());
//        causes unexpected looping with SEQUENCER up and dropping FORWARD requests warnings
//                .addProtocol(new FLUSH());
        stack.init();
        return channel;
    }
}
