/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package trackerserverutfpr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author giova
 */
public class processingModule extends Thread {

    public ArrayBlockingQueue listMsgs;
    public ArrayBlockingQueue listMsgsProcessed;
    
    public processingModule(ArrayBlockingQueue listMsgs, ArrayBlockingQueue listMsgsProcessed) {
        this.listMsgs = listMsgs;
        this.listMsgsProcessed = listMsgsProcessed;
    }
    
    @Override
    public void run() {
        while (true) {

            //REMOVE TODAS MENSAGENS DO ARRAY COMPARTILHADO
            ArrayList<String> list = removeMsgs();
            try {

                //INSERE TODAS MENSAGENS SEM PROCESSAR NO BANCO                
                insertMsgs(list);
                //System.out.println("Added all message not processed in database");

                //CRIA POOL DE THREADS PARA PROCESSAR
                ExecutorService es = createThreads(list);
                if (es != null) {
                    //ESPERA POOL DE THREADS TERMINAR
                    waitToProcess(es);
                }

                //REMOVE TODAS MENSAGENS PROCESSADAS DO ARRAY COMPARTILHADO
                ArrayList<TrackerInterface> listProcessed = removeMsgsProcessed();

                //INSERE TODAS MENSAGENS PROCESSADAS NO BANCO
                insertMsgsProcessed(listProcessed);

            } catch (SQLException ex) {
                Logger.getLogger(processingModule.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(processingModule.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ParseException ex) {
                Logger.getLogger(processingModule.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private ArrayList<String> removeMsgs() {
        ArrayList<String> list = new ArrayList<>();
        this.listMsgs.drainTo(list);
        return list;
    }
    
    private void insertMsgs(ArrayList<String> list) throws SQLException {
        if (!list.isEmpty()) {
            System.out.println("Size list not processed: " + list.size());
            try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
                connection.setAutoCommit(false);
                PreparedStatement ps = connection.prepareStatement("INSERT INTO message_received (content, processed) VALUES (?, ?)");
                for (String msgs : list) {
                    ps.setString(1, msgs);
                    ps.setBoolean(2, false);
                    ps.addBatch();
                }
                ps.executeBatch();
                connection.commit();
            }
        }
    }
    
    private ExecutorService createThreads(ArrayList<String> list) {
        if (!list.isEmpty()) {
            System.out.println("Created pool of threads for process messages");
            ArrayList<TrackerST300> tarefas = new ArrayList<>();
            list.forEach((msg) -> {
                TrackerST300 tracker = new TrackerST300(msg, listMsgsProcessed);
                tarefas.add(tracker);
            });
            //ExecutorService threadPool = Executors.newFixedThreadPool(4);
            ExecutorService threadPool = Executors.newCachedThreadPool();
            tarefas.forEach((tarefa) -> {
                threadPool.execute(tarefa);
            });
            return threadPool;
        } else {
            return null;
        }
    }
    
    private void waitToProcess(ExecutorService es) throws InterruptedException {
        if (es.isTerminated()) {
            System.out.println("Processed all tasks");
        }
        es.shutdown();
        System.out.println("Killed pool of threads");
    }
    
    private ArrayList<TrackerInterface> removeMsgsProcessed() {
        ArrayList<TrackerInterface> listForProcessed = new ArrayList<>();
        this.listMsgsProcessed.drainTo(listForProcessed);
        return listForProcessed;
    }
    
    private void insertMsgsProcessed(ArrayList<TrackerInterface> list) throws SQLException, ParseException {
        if (!list.isEmpty()) {
            System.out.println("Size list msgs processed: " + list.size());
            try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
                connection.setAutoCommit(false);
                PreparedStatement ps = connection.prepareStatement("INSERT INTO message_processed (tracker_id, time, latitude, longitude) VALUES (?, ?, ?, ?)");
                for (TrackerInterface tracker : list) {
                    Calendar c = Calendar.getInstance();
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                    c.setTime(format.parse(tracker.getDateTime()));
                    Timestamp stamp = new Timestamp(c.getTimeInMillis());

                    ps.setString(1, tracker.getIdTracker());
                    ps.setTimestamp(2, stamp);
                    ps.setString(3, tracker.getLatitude());
                    ps.setString(4, tracker.getLongitude());
                    ps.addBatch();
                }
                ps.executeBatch();
                connection.commit();
            }
            System.out.println("Added all message processed in database");
        }
    }


}
