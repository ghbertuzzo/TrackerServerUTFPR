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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProcessingModule implements Runnable {

    public ArrayBlockingQueue<TrackerST300> listMsgsProcessed;
    public ExecutorService threadPool;
    public int timeSleep;
    public int cicle, sizeSelect, sizeProcessed;
    public long startTime, endTime;

    public ProcessingModule(int time) {
        this.listMsgsProcessed = new ArrayBlockingQueue<>(100000);
        this.timeSleep = time;
        this.threadPool = Executors.newCachedThreadPool();
        this.cicle = 0;
        this.sizeSelect = 0;
        this.sizeProcessed = 0;
    }

    @Override
    public void run() {
        BufferedWriter bw = openLog();
        while (true) {
            startTime = System.currentTimeMillis();
            //SELECT PARA PEGAR TODAS MSG NAO PROCESSSADAS
            ArrayList<TrackerST300> list = getMsgsInDB();
            sizeSelect = list.size();
            if (!list.isEmpty()) {

                //UPDATE PARA ESTADO PROCESSANDO (processed=2)
                try {
                    updateToProcess(list);
                } catch (SQLException ex) {
                    Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
                }

                //PROCESSA MENSAGENS NO POOL
                processInPool(list, threadPool);

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
            endTime = System.currentTimeMillis();
            registerLog(bw);

            //ESPERA N SEG PARA REPETIR O CICLO
            try {
                sleep(this.timeSleep * 1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void processInPool(ArrayList<TrackerST300> list, ExecutorService threadPool) {
        list.forEach((track) -> {
            threadPool.execute(track);
        });
    }

    private void sleep(int n) throws InterruptedException {
        Thread.sleep(n);
    }

    private ArrayList<TrackerST300> removeMsgsProcessed() {
        ArrayList<TrackerST300> listForProcessed = new ArrayList<>();
        this.listMsgsProcessed.drainTo(listForProcessed);
        return listForProcessed;
    }

    private ArrayList<TrackerST300> getMsgsInDB() {
        ArrayList<TrackerST300> list = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
            PreparedStatement st = connection.prepareStatement("SELECT number_id, content FROM message_received WHERE processed=0");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                String id = rs.getString("number_id");
                String content = rs.getString("content");
                TrackerST300 track = new TrackerST300(content, listMsgsProcessed, id);
                list.add(track);
            }
            rs.close();
            st.close();
        } catch (SQLException e) {
            System.out.println("Connection failure");
        }
        return list;
    }

    private int[] insertAndUpdateMsgsProcessed(ArrayList<TrackerST300> listProcessed, ArrayList<TrackerST300> list) throws SQLException, ParseException {
        int[] retUpdate;
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement("INSERT INTO message_processed (tracker_id, time, latitude, longitude, time_receive) VALUES (?, ?, ?, ?, ?)");
            for (TrackerInterface tracker : listProcessed) {
                Calendar c = Calendar.getInstance();
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                c.setTime(format.parse(tracker.getDateTime()));
                Timestamp stamp = new Timestamp(c.getTimeInMillis());
                ps.setString(1, tracker.getIdTracker());
                ps.setTimestamp(2, stamp);
                ps.setString(3, tracker.getLatitude());
                ps.setString(4, tracker.getLongitude());
                Calendar calendar = Calendar.getInstance();
                java.util.Date now = calendar.getTime();
                java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(now.getTime());
                ps.setTimestamp(5, currentTimestamp);
                ps.addBatch();
            }
            int[] retInsert = ps.executeBatch();
            PreparedStatement ps2 = connection.prepareStatement("UPDATE message_received set processed=1 where number_id=?");
            for (TrackerST300 tracker : list) {
                ps2.setInt(1, Integer.parseInt(tracker.getIdDB()));
                ps2.addBatch();
            }
            retUpdate = ps2.executeBatch();
            connection.commit();
            System.out.println("Processing Module: Size Insert: " + retInsert.length + " Size Update: " + retUpdate.length);
        }
        return retUpdate;
    }

    private void updateToProcess(ArrayList<TrackerST300> list) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://172.17.0.3:5432/", "postgres", "utfsenha")) {
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement("UPDATE message_received set processed=2 where number_id=?");
            for (TrackerST300 tracker : list) {
                ps.setInt(1, Integer.parseInt(tracker.getIdDB()));
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
        }
    }

    private void registerLog(BufferedWriter bw) {
        try {
            bw.newLine();
            bw.write(cicle + ";" + (endTime - startTime) + ";" + sizeSelect + ";" + sizeProcessed);
            bw.flush();
        } catch (IOException ex) {
            Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
        }
        sizeProcessed = 0;
        cicle++;
    }

    private BufferedWriter openLog() {
        FileWriter fw = null;
        try {
            fw = new FileWriter("/home/Giovani/2019/TCC2/TrackerServerUTFPR/src/log.txt", true);
        } catch (IOException ex) {
            Logger.getLogger(ProcessingModule.class.getName()).log(Level.SEVERE, null, ex);
        }
        BufferedWriter bw = new BufferedWriter(fw);
        return bw;
    }
}
