package trackerserverutfpr;

public class MainProcessingModule {
    
    public static void main(String[] args){
        // TODO code application logic here        
        int timeSleep = 1; //IN SECONDS
        
        Thread threadProcModule = null;        
        ProcessingModule procModule = new ProcessingModule(timeSleep);
        threadProcModule = new Thread(procModule);
        threadProcModule.start();
    }
}