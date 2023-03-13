package lvc.cds.raft;

import lvc.cds.raft.proto.*;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCImplBase;

public class RaftRPC extends RaftRPCImplBase {
    ConcurrentLinkedQueue<Message> messages;


    public RaftRPC(ConcurrentLinkedQueue<Message> messages) {
        this.messages = messages;
    }

    @Override
    public void appendEntries(AppendEntriesMessage req, StreamObserver<Response> responseObserver) {
        String entries = "";
        for(int i = 0; i < req.getEntriesCount(); i++){
            if(i == req.getEntriesCount() - 1){
                entries += req.getEntries(i);
            } 
            else{
                entries += req.getEntries(i) + " ";
            }
        }
        
        String msg = "" + req.getTerm() + " " + req.getLeaderCommitIdx() + " "
                    + req.getPrevLogIdx() + " " + req.getPrevLogTerm() + "\n"
                    + req.getLeaderID() + "\n"
                    + entries;

        messages.add(new Message(msg));

        // int term = RaftNode.currentTerm;  make the variables public so this class can access them

        boolean success = false; // changes to true on the following if statement

        /**
         * get the last entry in the log and compare its idx and term
         * if the prevLogIdx and prevLogTerm match that of the follower we are good     (good as in we can implement the updates in entries)
         * we then set success to true 
         * if either of those are not true then we set success to false because our log does not 
         * match the leaders log up until the new entries....and we have to (fix that?)
         */

        /**
         * TODO would we then actually implement these changes inside the follower when we poll from messages??
         */
        
        Response reply = Response.newBuilder().setSuccess(success).setTerm(-1).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
    
    
    @Override
    public void requestVote(RequestVoteMessage req, StreamObserver<Response> responseObserver) {
        String msg = "" + req.getTerm() + " " + req.getCandidateID() + " " + req.getLastLogIndex() + " "
                    + req.getLastLogTerm();
        
        messages.add(new Message(msg));

        // int term = RaftNode.currentTerm;  make the variables public so this class can access them

        boolean votedFor = false;

        /**
         * if follower's term > candidates term reply false
         * check to make sure the logs are up to date with eachother, if they are grant the vote
         */

        /**
         * nothing to update for the follower until the cadidate becomes leader?
         */


        Response reply = Response.newBuilder().setSuccess(votedFor).setTerm(-1).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    // method for the clientRequest
    
    @Override
    public void clientRequest(ClientMessage req, StreamObserver<Empty> responseObserver) {
        
    }

    
}
