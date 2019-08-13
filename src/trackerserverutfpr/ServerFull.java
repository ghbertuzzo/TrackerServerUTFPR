/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package trackerserverutfpr;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 *
 * @author giova
 */
public class ServerFull {
    
    public ArrayBlockingQueue<String> listMsgs;
    public ArrayBlockingQueue<TrackerInterface> listMsgsProcessed;
    public ServerSocket serverSocket;
    private final int port;
    public Map<Socket, ThreadTracker> mapTrackers;     // Hash(socket|thread responsavel)

    public ServerFull(int port) throws IOException {
        this.port = port;
        this.listMsgs = new ArrayBlockingQueue<>(1000);
        this.listMsgsProcessed = new ArrayBlockingQueue<>(1000);
        this.mapTrackers = new HashMap<>();
    }
    
    public void start() throws IOException{
        Thread threadCapModule = null;
        Thread threadProcModule = null;
        
        captureModule capModule = new captureModule(this.port, this.serverSocket, this.mapTrackers, this.listMsgs);
        threadCapModule = new Thread(capModule);
        threadCapModule.start();
        
        processingModule procModule = new processingModule(this.listMsgs, this.listMsgsProcessed);
        threadProcModule = new Thread(procModule);
        threadProcModule.start();
    }
    
    
}
