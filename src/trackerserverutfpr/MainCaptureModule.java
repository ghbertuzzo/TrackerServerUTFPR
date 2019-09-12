package trackerserverutfpr;

import java.io.IOException;

public class MainCaptureModule {

    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        int httpPortNumber = 19880;
        int timeSleep = 1; //IN SECONDS

        Thread threadCapModule = null;
        CaptureModule capModule = new CaptureModule(httpPortNumber, timeSleep);
        threadCapModule = new Thread(capModule);
        threadCapModule.start();
    }

}
