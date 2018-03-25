package mat.distmap;

import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE_TRANSFER;
import org.jgroups.stack.ProtocolStack;

import java.net.InetAddress;
import java.util.Scanner;

public class Runner {

    public static void main(String[] args) throws Exception {
        new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.1"));
        JChannel channel = initializeChannel();
        String clusterName = "mateusz-home";
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
                map.put(message[1], message[2]);
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

    private static JChannel initializeChannel() throws Exception {
        JChannel channel = new JChannel(false);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        stack.addProtocol(new UDP())
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
                .addProtocol(new STATE_TRANSFER());
        stack.init();
        return channel;
    }
}
