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
public class captureModule implements Runnable {

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

    @Override
    public void run() {
        try {        
            this.serverSocket = new ServerSocket(port);
        } catch (IOException ex) {
            Logger.getLogger(captureModule.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("Starting the socket server at port:" + port);
        Socket client = null;  
        while (true) {         
            try {            
                client = this.serverSocket.accept();
            } catch (IOException ex) {
                Logger.getLogger(captureModule.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("New Client");
            Thread thread = null;
            if (!this.mapTrackers.containsKey(client)) {
                ThreadTracker tracker = new ThreadTracker(client, this.listMsgs);                
                thread = new Thread(tracker);
                thread.start();
                this.mapTrackers.put(client, tracker);
            }
        }
    }
    
}
