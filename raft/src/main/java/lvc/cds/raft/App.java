
package lvc.cds.raft;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class App {
    public static final int PORT = 5777;

    public static void main(String[] args) {

        try {
            String me = InetAddress.getLocalHost().getHostAddress();
            System.out.println(me);
            ArrayList<String> peers = new ArrayList<>();
            peers.add("10.1.23.77");
            peers.add("10.1.23.78");
            peers.add("10.1.23.79");

            RaftNode node = new RaftNode(PORT, me, peers);

            node.run();
            
        } catch (UnknownHostException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
}
