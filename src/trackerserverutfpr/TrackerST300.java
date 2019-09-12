package trackerserverutfpr;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TrackerST300 implements TrackerInterface, Runnable {

    public ArrayBlockingQueue<TrackerST300> msgsProcessed;
    public String msgcomplet;
    public String datetime;
    public String latitude;
    public String longitude;
    public String idtracker;
    private String modelDevice;
    private String softwareVersion;
    private String speed;
    private String distance;
    private String idDB;

    public TrackerST300(String msg, ArrayBlockingQueue<TrackerST300> msgsProcessed, String id) {
        this.msgcomplet = msg;
        this.msgsProcessed = msgsProcessed;
        this.idDB = id;
    }

    public TrackerST300(String idTracker, String dateTime, String latitu, String longitu, String idDB) {
        this.idtracker = idTracker;
        this.datetime = dateTime;
        this.latitude = latitu;
        this.longitude = longitu;
        this.idDB = idDB;
    }

    @Override
    public String getDateTime() {
        return this.datetime;
    }

    @Override
    public String getLatitude() {
        return this.latitude;
    }

    @Override
    public String getLongitude() {
        return this.longitude;
    }

    @Override
    public String getIdTracker() {
        return this.idtracker;
    }

    public String getModelDevice() {
        return this.modelDevice;
    }

    public String getSoftwareVersion() {
        return this.softwareVersion;
    }

    public String getSpeed() {
        return this.speed;
    }

    public String getDistance() {
        return this.distance;
    }

    public String getIdDB() {
        return this.idDB;
    }

    public String getMsgcomplet() {
        return msgcomplet;
    }

    public void setMsgcomplet(String msgcomplet) {
        this.msgcomplet = msgcomplet;
    }

    public static String processDateTime(String date, String time) {
        //;20190219;18:13:23;
        //2019-02-19 18:13:23
        String ano = date.substring(0, 4);
        String mes = date.substring(4, 6);
        String dia = date.substring(6, 8);
        String retorno = ano + "-" + mes + "-" + dia + " " + time;
        return retorno;
    }

    @Override
    public void run() {
        String[] msgparsed = this.msgcomplet.split(";");
        boolean processed = false;
        if (msgparsed.length > 0) {
            switch (msgparsed[0]) {
                case "ST300STT":
                    //TIPO DE MENSAGEM DE STATUS
                    //ST300STT;907036105;04;706;20190219;18:13:23;15800;-24.028422;-052.431049;
                    //049.560;119.29;12;1;20847760;27.71;100000;2;2003;079231;4.1;0;0;00000000000000;0
                    this.idtracker = msgparsed[1];
                    this.modelDevice = msgparsed[2];
                    this.softwareVersion = msgparsed[3];
                    this.datetime = processDateTime(msgparsed[4], msgparsed[5]);
                    this.latitude = msgparsed[7];
                    this.longitude = msgparsed[8];
                    this.speed = msgparsed[9]; // in km/h
                    this.distance = msgparsed[13]; // in m
                    processed = true;
                    break;
                case "ST300EMG":
                    //TIPO DE MENSAGEM DE EMG
                    //ST300EMG;907036105;04;706;20190409;00:22:00;56e425;-24.049020;-052.395404;
                    //000.011;000.00;10;1;27023573;0.00;000000;3;101149;4.3;0;0;00000000000000;0
                    this.idtracker = msgparsed[1];
                    this.modelDevice = msgparsed[2];
                    this.softwareVersion = msgparsed[3];
                    this.datetime = processDateTime(msgparsed[4], msgparsed[5]);
                    this.latitude = msgparsed[7];
                    this.longitude = msgparsed[8];
                    this.speed = msgparsed[9]; // in km/h
                    this.distance = msgparsed[13]; // in m
                    processed = true;
                    break;
                case "ST300EVT":
                    //TIPO DE MENSAGEM DE EVT
                    //ST300EVT;907036105;04;706;20190409;00:22:00;56e425;-24.049020;-052.395404;
                    //000.011;000.00;10;1;27023573;0.00;000000;3;101149;4.3;0;0;00000000000000;0
                    this.idtracker = msgparsed[1];
                    this.modelDevice = msgparsed[2];
                    this.softwareVersion = msgparsed[3];
                    this.datetime = processDateTime(msgparsed[4], msgparsed[5]);
                    this.latitude = msgparsed[7];
                    this.longitude = msgparsed[8];
                    //this.speed = msgparsed[9]; // in km/h
                    //this.distance = msgparsed[13]; // in m
                    processed = true;
                    break;
                case "ST300ALT":
                    //TIPO DE MENSAGEM DE ALT
                    //ST300ALT;907036105;04;706;20190410;16:54:56;56e440;-24.051715;-052.404470;
                    //020.776;239.55;12;1;27336146;27.54;100000;46;102207;4.2;1;0;00000000000000;0;14.64
                    this.idtracker = msgparsed[1];
                    this.modelDevice = msgparsed[2];
                    this.softwareVersion = msgparsed[3];
                    this.datetime = processDateTime(msgparsed[4], msgparsed[5]);
                    this.latitude = msgparsed[7];
                    this.longitude = msgparsed[8];
                    this.speed = msgparsed[9]; // in km/h
                    this.distance = msgparsed[13]; // in m
                    processed = true;
                    break;
                case "ST300HTE":
                    //TIPO DE MENSAGEM DE HTE - TRAVEL EVENT
                    this.idtracker = msgparsed[1];
                    this.modelDevice = msgparsed[2];
                    this.softwareVersion = msgparsed[3];
                    this.datetime = processDateTime(msgparsed[4], msgparsed[5]);
                    //this.latitude = msgparsed[7];
                    //this.longitude = msgparsed[8];
                    //this.speed = msgparsed[9]; // in km/h
                    //this.distance = msgparsed[13]; // in m
                    processed = false;
                    break;
                case "ST300ALV":
                    //TIPO DE MENSAGEM DE ALV - keep alive
                    //ST300ALV;907036105
                    this.idtracker = msgparsed[1];
                    this.datetime = null;
                    this.latitude = null;
                    this.longitude = null;
                    processed = false;
                    break;
                case "ST300UEX":
                    //TIPO DE MENSAGEM DE UEX
                    this.idtracker = msgparsed[1];
                    this.modelDevice = msgparsed[2];
                    this.softwareVersion = msgparsed[3];
                    this.datetime = processDateTime(msgparsed[4], msgparsed[5]);
                    this.latitude = msgparsed[7];
                    this.longitude = msgparsed[8];
                    //this.speed = msgparsed[9]; // in km/h
                    //this.distance = msgparsed[13]; // in m
                    processed = true;
                    break;
                case "ST300CMD":
                    //TIPO DE MENSAGEM DE CMD
                    this.idtracker = msgparsed[1];
                    this.datetime = null;
                    this.latitude = null;
                    this.longitude = null;
                    processed = false;
                    break;
                default:
                    break;
            }
        }
        if (processed) {
            TrackerST300 tracker = new TrackerST300(this.idtracker, this.datetime, this.latitude, this.longitude, this.idDB);
            try {
                this.msgsProcessed.put(tracker);
            } catch (InterruptedException ex) {
                Logger.getLogger(TrackerST300.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
