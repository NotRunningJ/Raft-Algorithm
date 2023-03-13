package lvc.cds.raft;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.io.File;

import java.util.AbstractMap;

import com.google.gson.JsonObject;

import org.json.JSONObject;
import org.json.JSONPointer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lvc.cds.KVS;
import lvc.cds.raft.proto.AppendEntriesMessage;
import lvc.cds.raft.proto.RaftRPCGrpc;
import lvc.cds.raft.proto.RequestVoteMessage;
import lvc.cds.raft.proto.Response;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCStub;





public class RaftNode {
    public enum NODE_STATE {
        FOLLOWER, LEADER, CANDIDATE, SHUTDOWN
    }

    private Random rand = new Random();
    private final int TIMEOUTMAX = 300; 
    private final int TIMEOUTMIN = 150;

    private ConcurrentLinkedQueue<Message> messages;

    private NODE_STATE state;
    private Server rpcServer;
    private Map<String, PeerStub> peers;
    private boolean peersConnected;
    
    private int currentTerm;
    private String votedFor; //TODO: Should this be an int?
    private ArrayList<AbstractMap.SimpleEntry<Integer, String>> log = new ArrayList();
    private int commitIndex;
    private int lastApplied;

    private String storagePath = Paths.get(System.getProperty("user.dir"), "perStorage.txt").toString();

    private List<String> peerIPs;
    private String selfIP;

    protected int port;

    protected RaftNode(int port, String me, List<String> peers) throws IOException {
        // incoming RPC messages come to this port
        this.port = port;
        this.messages = new ConcurrentLinkedQueue<>();
        this.peerIPs = new ArrayList<>();
        this.selfIP = me;
        
        // a map containing stubs for communicating with each of our peers
        this.peers = new HashMap<>();
        for (var p : peers) {
            if (!p.equals(me))
                this.peers.put(p, new PeerStub(p, port, messages));
                this.peerIPs.add(p);
        }
        // lazily connect to peers.
        this.peersConnected = false;
        
        this.state = NODE_STATE.FOLLOWER;
        
    }

    public void run() throws IOException {
        // start listening for incoming messages now, so that others can connect
        startRpcListener();

        // note: we defer making any outgoing connections until we need to send
        // a message. This should help with startup.

        // a state machine implementation.
        state = NODE_STATE.FOLLOWER;

        File storage = new File(storagePath);
        storage.createNewFile();

        if(storage.length() == 0){
            KVS storedState = new KVS();
            JSONObject stJson = new JSONObject();
            stJson.put("currentTerm", 0);
            stJson.put("votedFor", "null");
            stJson.put("log", "");
            storedState.put("Local Storage", stJson);
            storedState.write(storagePath);
        }else{
            KVS storedState = new KVS();
            storedState.read(storagePath);
            currentTerm = (Integer)storedState.getJSON("Local Storage", "currentTerm");
            votedFor = storedState.getJSON("Local Storage", "votedFor").equals("null") ? null : (String)storedState.getJSON("Local Storage", "votedFor");
            
            String pairs = (String)storedState.getJSON("Local Storage", "log");
            String[] keys = pairs.split("\n");

            for(String element : keys){
                int term = Integer.parseInt(element.split(":")[0]);
                String value = element.split(":")[1];
                AbstractMap.SimpleEntry entry = new AbstractMap.SimpleEntry<>(term, value);
                log.add(entry);
            }
        }
        
        commitIndex = 0;
        lastApplied = 0;

        while (state != NODE_STATE.SHUTDOWN) {
            switch (state) {
            case FOLLOWER:
                state = follower();
                break;
            case LEADER:
                state = leader();
                break;
            case CANDIDATE:
                state = candidate();
                break;
            case SHUTDOWN:
                break;
            }
        }

        // shut down the server
        try {
            shutdownRpcListener();
            shutdownRpcStubs();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void connectRpcChannels() {
        for (var peer : peers.values()) {
            peer.connect();
        }
    }

    private void shutdownRpcStubs() {
        try {
            for (var peer : peers.values()) {
                peer.shutdown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startRpcListener() throws IOException {
        // build and start the server to listen to incoming RPC calls.
        rpcServer = ServerBuilder.forPort(port)
                .addService(new RaftRPC(messages))
                .build()
                .start();

        // add this hook to be run at shutdown, to make sure our server
        // is killed and the socket closed.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdownRpcListener();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private void shutdownRpcListener() throws InterruptedException {
        if (rpcServer != null) {
            rpcServer.shutdownNow().awaitTermination();
        }
    }

    private NODE_STATE follower() {

        long timeout = 500 + rand.nextInt(TIMEOUTMAX);
        long lastContact = System.currentTimeMillis();
        while (timeout > System.currentTimeMillis() - lastContact) {

            // See if there are incoming messages 
            // or replies to process
            Message m = messages.poll();
            if (m != null) {
                String[] splitmsg = m.msg.split(":");
                if(splitmsg.length != 4) continue;
                if (m.msg.equals("")) { //TODO change to test if entries is empty
                    System.out.println("hearbeat ♡"); //TODO: remove this for production?
                } else {
                    // some real interesting message
                    if(peerIPs.contains(splitmsg[1])) {
                        if(splitmsg[0].equals("reply from")) {
                            // msg from leader
                            boolean success = true;
                            //TODO how to read the actual appendEntries (to get pregLogIndex)
                            if(Integer.parseInt(splitmsg[3]) < currentTerm) {
                            success = false;
                            }
                            // if(log.size() > prevLogIndex) {
                            //     if(log.get(prevLogIndex).getKey() != prevLogTerm) {
                            //         success = false;
                            //     }
                            // } else {
                            //     success = false;
                            // }
                            // read in new entries and add to the log
                            // for (int i = 0; i < entries.size(); i++) { //TODO access entries..
                            //     // get the entry at the index
                            //     // check if the index & term are already in the log, 
                            //     // if they are, remove that entry from the log and enything else ahead of it

                            //     // add the entry to the end of the log
                            // }
                            // if (leaderCommit > commitIndex) {
                            //     commitIndex = min(leaderCommit, log.size()-1);
                            // }
                            // reply with currentTerm and success variables.
                            //TODO how to reply to messages??
                        } else {
                            // msg from candidate 
                            boolean voteGranted = true;
                            // if(term < currentTerm) {
                            //     voteGranted = false;
                            // }
                            // if (votedFor != null) {
                            //     voteGranted = false;
                            // }
                            // if(lastLogTerm < currentTerm) {
                            //     voteGranted = false;
                            // } else {
                            //     if(lastLogIndex < log.size()-1) {
                            //         voteGranted = false;
                            //     }
                            // }
                            // reply with term and voteGranted

                            //TODO how to reply to messages??
                        }
                    }
                }

                System.out.println("handled message");
                System.out.println(m.msg);

                lastContact = System.currentTimeMillis();
            }

            // If we haven't connected yet...
            if (!peersConnected) {
                connectRpcChannels();
            }

        }
        // timeout expired, become Candidate
        currentTerm++;
        return NODE_STATE.CANDIDATE;
    }

    private NODE_STATE leader() {
        // any setup that is needed before we start out event loop as we just
        // became leader. For instance, initialize the nextIndex and matchIndex
        // hashmaps.

        HashMap<String, Integer> nextIndex = new HashMap<>();
        HashMap<String, Integer> matchIndex = new HashMap<>();
        for (String p : peerIPs) {
            nextIndex.put(p, log.size() + 1);
            matchIndex.put(p, 0);
        }




        // notes: We have decisions to make regarding things like: how many queues do
        // we want (one is workable, but several (e.g., one for client messages, one 
        // for incoming raft messages, one for replies) may be easier to reason about).
        // We also need to determine what type of thing goes in a queue -- we could
        // for instance use a small inheritance hierarchy (a base class QueueEntry with 
        // derived classes for each type of thing that can be enqueued), or make enough
        // queues so that each is homogeneous. Or, a single type of queue entry with 
        // enough fields in it that it could hold any type of message, and facilities in 
        // place to determine what we're looking at.



        while (true) {
            // step one: check out commitIndex to see if we can commit any
            // logs. As we commit logs, Send message to client that the job is done 
            // for that log entry.Increase lastApplied
            //
            // step 2: check to see if any messages or replies are present
            // if so:
            //    - if it is a request from a client, then add to our log
            //    - if it is a response to an appendEntries, process it
            //      by first checking the term, then (if success is true)
            //      incrementing nextIndex and matchIndex for that node or
            //      (if success is false) decrementing nextIndex for the follower.
            //    - if it is anything else (e.g., a requestVote or appendEntries)  
            //      then we want to check the term, and if appropriate, convert to
            //      follower (leaving this message on the queue!)
            // 
            // step 3: see if we need to send any messages out. Iterate through
            //         nextIndex and send an appendEntries message to anyone who 
            //         seems out of date. If our heartbeat timeout has expired, send
            //         a message to everyone. if we do send an appendEntries, 
            //         update the heartbeat timer.
            //
            // step 4: iterate through matchIndex to see if a majority of entries
            //         are > commitIndex (with term that is current). If so, 
            //         ++commitIndex. There are more aggressive ways to do this,
            //         but this will do.
        }
    }

    private NODE_STATE candidate() {
        // an event loop that processes incoming messages and timeout events
        // according to the raft rules for followers.

        int voteCount = 1;
        votedFor = selfIP;
        for(PeerStub peer : peers.values()){
            peer.sendRequestVote(currentTerm, selfIP, log.size(), log.isEmpty() ? 0 : log.get(log.size() - 1).getKey());
        }

        //TODO: Send out RequestVote RPCS to all servers

        long timeout = 500 + rand.nextInt(TIMEOUTMAX);
        long lastContact = System.currentTimeMillis();
        while (timeout > System.currentTimeMillis() - lastContact) {

            // first, see if there are incoming messages or replies
            // to process
            Message m = messages.poll();
            if (m != null) {
                String[] splitmsg = m.msg.split(":");
                if(splitmsg.length != 4) continue;
                if(peerIPs.contains(splitmsg[1])){
                    if(currentTerm == Integer.parseInt(splitmsg[2])){
                        if(splitmsg[0].equals("reply from")){
                            return NODE_STATE.FOLLOWER;
                        }
                        else{
                            voteCount += Boolean.parseBoolean(splitmsg[3]) ? 1 : 0;
                        }
                    }
                    else if(currentTerm < Integer.parseInt(splitmsg[2])) return NODE_STATE.FOLLOWER;
                }
                if(voteCount > peerIPs.size() / 2.0){
                    return NODE_STATE.LEADER;
                }
                

                /*
                For each message, 
                if message is successsful for, increment vote count
                    if voteCount > cluster.size/2
                        return NODE_STATE.LEADER;

                else if message is AppendEntries RPC,
                    if other server term >= my term, acknowlegde their leadership, become follower
                    else reject and continue candidacy

                update last contact

                */
                lastContact = System.currentTimeMillis();
            }
            // Election has failed, incrememt term and restart as candidate. 

            //TODO: do we need to reset to Candidate here if election has failed?
            
        }
        currentTerm++;
        return NODE_STATE.CANDIDATE;
    }

    private static class PeerStub {
        ConcurrentLinkedQueue<Message> messages;
        String peer;
        int port;

        ManagedChannel channel;
        RaftRPCStub stub;

        PeerStub(String peer, int port, ConcurrentLinkedQueue<Message> messages) {
            this.peer = peer;
            this.port = port;
            this.channel = null;
            this.stub = null;
            this.messages = messages;
        }

        void connect() {
            channel = ManagedChannelBuilder.forAddress(peer, port).usePlaintext().build();
            stub = RaftRPCGrpc.newStub(channel);
        }

        RaftRPCStub getStub() {
            return stub;
        }

        void shutdown() throws InterruptedException {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

        void sendAppendEntries(int term, String leaderID, int prevLogIdx, int prevLogTerm,
                            ArrayList<String> entries, int leaderCommitIndex) {
            AppendEntriesMessage request = AppendEntriesMessage.newBuilder()
            .setTerm(term).setLeaderID(leaderID)
            .setPrevLogIdx(prevLogIdx).setPrevLogTerm(prevLogTerm)
            .addAllEntries(entries)
            .setLeaderCommitIdx(leaderCommitIndex)
            .build();

            getStub().appendEntries(request, new StreamObserver<Response>() {
                @Override
                public void onNext(Response value) {
                    // we have the peer string (IP address) available here.
                    String msg = "reply from:" + peer + ":" + value.getTerm() + ":" + value.getSuccess();
                    messages.add(new Message(msg));
                }

                @Override
                public void onError(Throwable t) {
                    messages.add(new Message("error handling response"));
                }

                @Override
                public void onCompleted() {
                    System.err.println("stream observer onCompleted");
                }
            });


        }

        void sendRequestVote(int term, String candidateID, int lastLogIndex, int lastLogTerm) {
            RequestVoteMessage request = RequestVoteMessage.newBuilder().setTerm(term)
                .setCandidateID(candidateID)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm).build();

            getStub().requestVote(request, new StreamObserver<Response>() {
                @Override
                public void onNext(Response value) {
                    // we have the peer string (IP address) available here.
                    String msg = "vote from:" + peer + ":" + value.getTerm() + ":" + value.getSuccess();
                    messages.add(new Message(msg));
                }

                @Override
                public void onError(Throwable t) {
                    messages.add(new Message("error handling response"));
                }

                @Override
                public void onCompleted() {
                    System.err.println("stream observer onCompleted");
                }
                
            });
        }
    }
}
