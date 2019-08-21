package trackerserverutfpr;

import java.io.IOException;

public class ServerFull {
    
    private final int port;

    public ServerFull(int port) throws IOException {
        this.port = port;     
    }
    
    public void start() throws IOException{
        Thread threadCapModule = null;
        Thread threadProcModule = null;
        
        CaptureModule capModule = new CaptureModule(this.port);
        threadCapModule = new Thread(capModule);
        threadCapModule.start();
        
        ProcessingModule procModule = new ProcessingModule();
        threadProcModule = new Thread(procModule);
        threadProcModule.start();
    }
    
    
}
