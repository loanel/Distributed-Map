package mat.distmap;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;

import java.util.List;

public class ViewHandler extends Thread {
    private JChannel ch;
    private MergeView view;

    ViewHandler(JChannel ch, MergeView view) {
        this.ch = ch;
        this.view = view;
    }

    /// api tutorial function for ViewHandler
    public void run() {
        List<View> subgroups = view.getSubgroups();
        View tmp_view = subgroups.get(0);
        Address local_addr = ch.getAddress();
        if (!tmp_view.getMembers().contains(local_addr)) {
            System.out.println("Not member of the new primary partition ("
                    + tmp_view + "), will re-acquire the state");
            try {
                ch.getState(null, 0);
            } catch (Exception ignored) {
            }
        } else {
            System.out.println("Not member of the new primary partition ("
                    + tmp_view + "), will do nothing");
        }
    }
}
