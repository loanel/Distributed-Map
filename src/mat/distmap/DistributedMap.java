package mat.distmap;


import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {
    private JChannel channel;
    private Map<String, String> dataset = new ConcurrentHashMap<>();

    DistributedMap(JChannel channel, String clusterName) throws Exception {
        this.channel = channel;
        channel.setReceiver(this);
        channel.connect(clusterName);
        channel.getState(null, 0);
    }

    @Override
    public boolean containsKey(String key) {
        return dataset.containsKey(key);
    }

    @Override
    public String get(String key) {
        return dataset.get(key);
    }

    @Override
    public String put(String key, String value) {
        String returnValue = null;
        if (containsKey(key)) {
            returnValue = get(key);
        }
        dataset.put(key, value);
        sendMessageToChannel("put " + key + " " + value);
        return returnValue;
    }

    @Override
    public String remove(String key) {
        String removed = dataset.remove(key);
        sendMessageToChannel("remove " + key);
        return removed;
    }

    private void sendMessageToChannel(String message) {
        byte[] byteString = message.getBytes();
        try {
            channel.send(new Message(null, null, byteString));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void receive(Message msg) {
        try {
            String[] messageText = new String(msg.getBuffer()).split("\\s+");
            parseMessage(messageText);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void parseMessage(String[] messageText) {
        String command = messageText[0];
        if (command.equals("put")) {
            dataset.put(messageText[1], messageText[2]);
        }
        if (command.equals("remove")) {
            dataset.remove(messageText[1]);
        }
    }


    @Override
    public void getState(OutputStream output) throws Exception {
        Util.objectToStream(dataset, new DataOutputStream(output));
        System.out.println("getState");
    }

    @Override
    public void setState(InputStream input) throws Exception {
        Map<String, String> newData = (ConcurrentHashMap<String, String>) Util.objectFromStream(new DataInputStream(input));
        dataset.clear();
        dataset.putAll(newData);
    }

    @Override
    public void viewAccepted(View new_view) {
        if (new_view instanceof MergeView) {
            System.out.println("handling MergeView");
            ViewHandler viewHandler = new ViewHandler(channel, (MergeView) new_view);
            viewHandler.start();
        }
    }

    void displayDataset() {
        for (String key : dataset.keySet()) {
            System.out.println(key + " " + dataset.get(key));
        }
    }
}
