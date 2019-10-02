package trackerserverutfpr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CaptureModuleInsertDB implements Runnable {

    public ArrayBlockingQueue<String> listMsgs;
    public int timeSleep;
    public int cicle, sizeArray, retInsert;
    public int[] sizeInsert;
    public long startTime, endTime;

    public CaptureModuleInsertDB(ArrayBlockingQueue<String> listMsgs, int timesl) {
        this.listMsgs = listMsgs;
        this.timeSleep = timesl;
        this.cicle = 0;
        this.sizeArray = 0;
        this.retInsert = 0;
        this.sizeInsert = null;
    }

    @Override
    public void run() {
        BufferedWriter bw = openLog();
        while (true) {
            startTime = System.currentTimeMillis();
            if (!this.listMsgs.isEmpty()) {
                //REMOVE MSGS DO ARRAY COMPARTILHADO
                ArrayList<String> array = removeMsgs();
                sizeArray = array.size();
                //INSERE MSGS NO BANCO
                try {
                    sizeInsert = insertMsgs(array);
                    retInsert = sizeInsert.length;
                } catch (SQLException ex) {
                    Logger.getLogger(CaptureModuleInsertDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            endTime = System.currentTimeMillis();
            registerLog(bw);
            try {
                sleep(1000 * this.timeSleep);
            } catch (InterruptedException ex) {
                Logger.getLogger(CaptureModuleInsertDB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void sleep(int n) throws InterruptedException {
        Thread.sleep(n);
    }

    private ArrayList<String> removeMsgs() {
        ArrayList<String> list = new ArrayList<>();
        this.listMsgs.drainTo(list);
        return list;
    }

    private int[] insertMsgs(ArrayList<String> list) throws SQLException {
        int[] retornoInsert = null;
        if (!list.isEmpty()) {
            try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
                connection.setAutoCommit(false);
                PreparedStatement ps = connection.prepareStatement("INSERT INTO message_received (content, processed, time) VALUES (?, ?, ?)");
                for (String msgs : list) {
                    ps.setString(1, msgs);
                    ps.setInt(2, 0);
                    Calendar calendar = Calendar.getInstance();
                    java.util.Date now = calendar.getTime();
                    java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
                    ps.setTimestamp(3, currentTimestamp);
                    ps.addBatch();
                }
                retornoInsert = ps.executeBatch();
                System.out.println("Capture Module: Size list inserted: " + retornoInsert.length);
                connection.commit();
            }
        }
        return retornoInsert;
    }

    private void registerLog(BufferedWriter bw) {
        try {
            bw.newLine();
            bw.write(cicle + ";" + (endTime - startTime) + ";" + sizeArray + ";" + retInsert);
            bw.flush();
        } catch (IOException ex) {
            Logger.getLogger(CaptureModuleInsertDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        cicle++;
        sizeArray = 0;
        retInsert = 0;
    }

    private BufferedWriter openLog() {
        FileWriter fw = null;
        try {
            fw = new FileWriter("/home/Giovani/2019/TCC2/TrackerServerUTFPR/src/log-cap.txt", true);
        } catch (IOException ex) {
            Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
        }
        BufferedWriter bw = new BufferedWriter(fw);
        return bw;
    }

}
