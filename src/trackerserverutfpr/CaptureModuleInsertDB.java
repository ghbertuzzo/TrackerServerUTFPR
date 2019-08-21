package trackerserverutfpr;

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

    public CaptureModuleInsertDB(ArrayBlockingQueue<String> listMsgs) {
        this.listMsgs = listMsgs;
    }
    
    @Override
    public void run() {
        System.out.println("Start Thread Capture Module Insert DB");
        while(true){
            if(!this.listMsgs.isEmpty()){
                ArrayList<String> array = removeMsgs();
                try {
                    insertMsgs(array);
                } catch (SQLException ex) {
                    Logger.getLogger(CaptureModuleInsertDB.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            try {
                sleep(1000);
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
    
    private void insertMsgs(ArrayList<String> list) throws SQLException{
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
    
}
