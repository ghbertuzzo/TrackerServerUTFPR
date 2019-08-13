package trackerserverutfpr;

import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Giovani Bertuzzo github.com/ghbertuzzo
 */
public class ThreadTracker implements Runnable {

    private final Socket client;
    public ArrayBlockingQueue<String> refSharedList;

    public ThreadTracker(Socket client, ArrayBlockingQueue<String> refSharedList) {
        this.client = client;
        this.refSharedList = refSharedList;
    }

    @Override
    public void run() {
        Scanner entrada = null;
        try {
            entrada = new Scanner(this.client.getInputStream());
        } catch (IOException ex) {
            Logger.getLogger(ThreadTracker.class.getName()).log(Level.SEVERE, null, ex);
        }
        while (entrada.hasNextLine()) {
            String msg = entrada.nextLine();
            System.out.println(msg);
            try {
                this.refSharedList.put(msg);
            } catch (InterruptedException ex) {
                System.out.println("Enter Catch Exception for put msg in ref shared list");
                Logger.getLogger(ThreadTracker.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Exited while stream");
    }
    
}
