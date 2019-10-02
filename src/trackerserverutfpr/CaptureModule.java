package trackerserverutfpr;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CaptureModule implements Runnable {

    public ServerSocket serverSocket;
    private final int port;
    public ArrayBlockingQueue<String> listMsgs;
    public int timeSleep;

    public CaptureModule(int port, int timesl) {
        this.port = port;
        this.listMsgs = new ArrayBlockingQueue<>(50000);
        this.timeSleep = timesl;
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
        CaptureModuleInsertDB capModule = new CaptureModuleInsertDB(this.listMsgs, this.timeSleep);
        threadCapModuleInsert = new Thread(capModule);
        threadCapModuleInsert.start();

        while (true) {
            try {
                client = this.serverSocket.accept();
            } catch (IOException ex) {
                Logger.getLogger(CaptureModule.class.getName()).log(Level.SEVERE, null, ex);
            }
            Thread thread = null;
            ThreadTracker tt = new ThreadTracker(client, listMsgs);
            thread = new Thread(tt);
            thread.start();
        }
    }

}
