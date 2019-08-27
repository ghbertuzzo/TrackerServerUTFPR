package trackerserverutfpr;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PoolProcessingModule implements Runnable {
    
    ArrayList<TrackerST300> list;
    ExecutorService threadPool;
    ArrayBlockingQueue msgsProcessed;

    public PoolProcessingModule(ArrayList<TrackerST300> list, ArrayBlockingQueue msgsProcessed) {
        this.list = list;
        this.msgsProcessed = msgsProcessed;
    }

    @Override
    public void run() {
        threadPool = Executors.newFixedThreadPool(4);
        //threadPool = Executors.newCachedThreadPool();
        createThreads(list, threadPool, msgsProcessed);
    }
    
    private void createThreads(ArrayList<TrackerST300> list, ExecutorService threadPool, ArrayBlockingQueue msgsProcessed) {
        list.forEach((track) -> {
            TrackerST300 tracker = new TrackerST300(track.getMsgcomplet(), msgsProcessed,track.getIdDB());
            threadPool.execute(tracker);
        });
        threadPool.shutdown();
    }
}
