package trackerserverutfpr;

import java.io.IOException;

public class ServerMain {

    public static void main(String[] args) throws IOException{
        // TODO code application logic here
        int httpPortNumber = 19880;
        
        Thread threadCapModule = null;
        Thread threadProcModule = null;
        
        CaptureModule capModule = new CaptureModule(httpPortNumber);
        threadCapModule = new Thread(capModule);
        threadCapModule.start();
        
        ProcessingModule procModule = new ProcessingModule();
        threadProcModule = new Thread(procModule);
        threadProcModule.start();
    }
    
}
