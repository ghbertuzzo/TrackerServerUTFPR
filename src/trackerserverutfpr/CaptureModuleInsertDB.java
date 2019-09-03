package trackerserverutfpr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CaptureModuleInsertDB implements Runnable {
    
    public ArrayBlockingQueue<String> listMsgs;
    public int timeSleep;

    public CaptureModuleInsertDB(ArrayBlockingQueue<String> listMsgs, int timesl) {
        this.listMsgs = listMsgs;
        this.timeSleep = timesl;
    }
    
    @Override
    public void run() {
        int cicle = 0;
        int sizeArray = 0;
        int[] sizeInsert = null;
        int retInsert = 0;
        long startTime,endTime;
        FileWriter fw = null;
        try {
            fw = new FileWriter("/home/Giovani/2019/TCC2/TrackerServerUTFPR/src/log-cap.txt",true);
        } catch (IOException ex) {
            Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
        }
        BufferedWriter bw = new BufferedWriter(fw);  
        while(true){
            cicle++;
            startTime = System.currentTimeMillis();
            if(!this.listMsgs.isEmpty()){
                ArrayList<String> array = removeMsgs();
                sizeArray = array.size();
                try {
                    sizeInsert = insertMsgs(array);
                    retInsert = sizeInsert.length;
                } catch (SQLException ex) {
                    Logger.getLogger(CaptureModuleInsertDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            endTime = System.currentTimeMillis();
            try {
                bw.newLine();
                bw.write(cicle+";"+(endTime-startTime)+";"+sizeArray+";"+retInsert);
                bw.flush();
            } catch (IOException ex) {
                Logger.getLogger(CaptureModuleInsertDB.class.getName()).log(Level.SEVERE, null, ex);
            }
            sizeArray = 0;
            retInsert = 0;
            try {
                sleep(1000*this.timeSleep);
            } catch (InterruptedException ex) {
                Logger.getLogger(CaptureModuleInsertDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private void sleep(int n) throws InterruptedException{
        Thread.sleep(n);
    }
    private ArrayList<String> removeMsgs() {
        ArrayList<String> list = new ArrayList<>();
        this.listMsgs.drainTo(list);
        return list;
    }
    
    private int[] insertMsgs(ArrayList<String> list) throws SQLException{
        int[] retInsert = null;
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
                retInsert = ps.executeBatch();
                connection.commit();
            }
        }
        return retInsert;
    }
    
}
