package trackerserverutfpr;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CaptureModule implements Runnable {

    public ServerSocket serverSocket;
    private final int port;
    public ExecutorService threadPool;
    public ArrayBlockingQueue<String> listMsgs;
    
    public CaptureModule(int port) {
        this.port = port;
        this.threadPool = Executors.newCachedThreadPool();
        this.listMsgs = new ArrayBlockingQueue<>(50000);
    }

    @Override
    public void run() {
        try {        
            this.serverSocket = new ServerSocket(port);
        } catch (IOException ex) {
            Logger.getLogger(CaptureModule.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("Starting the socket server at port:" + port);
        Socket client = null;  
        
        Thread threadCapModuleInsert = null;                
        CaptureModuleInsertDB capModule = new CaptureModuleInsertDB(this.listMsgs);
        threadCapModuleInsert = new Thread(capModule);
        threadCapModuleInsert.start();
        
        while (true) {  
            try {
                client = this.serverSocket.accept();
            } catch (IOException ex) {
                Logger.getLogger(CaptureModule.class.getName()).log(Level.SEVERE, null, ex);
            }
            this.threadPool.execute(new ThreadTracker(client, listMsgs));
        }
    }
    
}
