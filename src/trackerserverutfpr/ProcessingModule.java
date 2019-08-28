package trackerserverutfpr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProcessingModule implements Runnable {

    public ArrayBlockingQueue<TrackerST300> listMsgsProcessed;
    public int timeSleep;
    
    public ProcessingModule(int time) {        
        this.listMsgsProcessed = new ArrayBlockingQueue<>(50000);   
        this.timeSleep = time;
    }
    
    @Override
    public void run() {
        int cicle = 0;
        int sizeSelect = 0;
        int sizeProcessed = 0;
        FileWriter fw = null;
        try {
            fw = new FileWriter("src/log.txt",true);
        } catch (IOException ex) {
            Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
        }
        BufferedWriter bw = new BufferedWriter(fw);        
        while (true) {
            cicle++;
            long startTime = System.currentTimeMillis();
            //SELECT PARA PEGAR TODAS MSG NAO PROCESSSADAS
            ArrayList<TrackerST300> list = getMsgsInDB();
            sizeSelect = list.size();
            if(!list.isEmpty()){
                
                //CRIA POOL DE THREADS PARA PROCESSAR MSGS
                PoolProcessingModule pool = new PoolProcessingModule(list, this.listMsgsProcessed);        
                Thread threadPool = null;        
                threadPool = new Thread(pool);
                threadPool.start();
                try {
                    threadPool.join();
                } catch (InterruptedException ex) {
                    Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                //REMOVE TODAS MENSAGENS PROCESSADAS DO ARRAY COMPARTILHADO
                ArrayList<TrackerST300> listProcessed = removeMsgsProcessed();

                //INSERE TODAS MENSAGENS PROCESSADAS NO BANCO E ATUALIZA MSGS NAO PROCESSADAS PARA PROCESSADAS
                try {                      
                    int[] retUpdate = insertAndUpdateMsgsProcessed(listProcessed, list);
                    sizeProcessed = retUpdate.length;
                } catch (SQLException | ParseException ex) {
                    Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
                }           
            }            
            long endTime = System.currentTimeMillis();
            //System.out.println(cicle+";"+startTime+";"+endTime+";"+sizeSelect+";"+sizeProcessed);
            try {
                bw.write(cicle+";"+startTime+";"+endTime+";"+sizeSelect+";"+sizeProcessed);
            } catch (IOException ex) {
                Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
            }
            //ESPERA 1 SEG PARA REPETIR O CICLO
            try {
                sleep(this.timeSleep*1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private void sleep(int n) throws InterruptedException{
        Thread.sleep(n);
    }
    
    private ArrayList<TrackerST300> removeMsgsProcessed() {
        ArrayList<TrackerST300> listForProcessed = new ArrayList<>();
        this.listMsgsProcessed.drainTo(listForProcessed);
        return listForProcessed;
    }
    
    private ArrayList<TrackerST300> getMsgsInDB() {
        ArrayList<TrackerST300> list = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")){
            PreparedStatement st = connection.prepareStatement("SELECT number_id, content FROM message_received WHERE processed=false");
            ResultSet rs = st.executeQuery();
            while (rs.next()){
                String id = rs.getString("number_id");
                String content = rs.getString("content");
                TrackerST300 track = new TrackerST300(content, listMsgsProcessed, id);
                list.add(track);
            }
            rs.close();
            st.close();
        }catch (SQLException e){
            System.out.println("Connection failure");         
        }        
        return list;
    }
    
    private int[] insertAndUpdateMsgsProcessed(ArrayList<TrackerST300> listProcessed, ArrayList<TrackerST300> list) throws SQLException, ParseException {        
        int[] retUpdate;
        System.out.println("Size list msgs processed: " + listProcessed.size());            
        System.out.println("Size list update: " + list.size());
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement("INSERT INTO message_processed (tracker_id, time, latitude, longitude) VALUES (?, ?, ?, ?)");
            for (TrackerInterface tracker : listProcessed) {
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
            int[] retInsert = ps.executeBatch();
            PreparedStatement ps2 = connection.prepareStatement("UPDATE message_received set processed=true where number_id=?");
            for (TrackerST300 tracker : list) {
                ps2.setInt(1, Integer.parseInt(tracker.getIdDB()));
                ps2.addBatch();
            }
            retUpdate = ps2.executeBatch();
            connection.commit();                
            System.out.println("Size Insert: "+retInsert.length+"\nSize Update: "+retUpdate.length);
        }
        return retUpdate;
    }
}
