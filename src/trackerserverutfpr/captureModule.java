/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package trackerserverutfpr;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author giova
 */
public class captureModule {

    public ServerSocket serverSocket;
    private final int port;
    public Map<Socket, ThreadTracker> mapTrackers; 
    public ArrayBlockingQueue<String> listMsgs;
    
    public captureModule(int port, ServerSocket serverSocket, Map<Socket, ThreadTracker> mapTrackers, ArrayBlockingQueue<String> listMsgs) {
        this.port = port;
        this.serverSocket = serverSocket;
        this.mapTrackers = mapTrackers;
        this.listMsgs = listMsgs;
    }

    public void start() throws IOException {
        this.serverSocket = new ServerSocket(port);        
        System.out.println("Starting the socket server at port:" + port);
        Socket client = null;  
        while (true) {         
            client = this.serverSocket.accept();            
            System.out.println("New Client");
            ThreadTracker tracker;
            if (!this.mapTrackers.containsKey(client)) {
                tracker = new ThreadTracker(client, this.listMsgs);
                tracker.run();
                this.mapTrackers.put(client, tracker);
            }
        }
    }
    
}
