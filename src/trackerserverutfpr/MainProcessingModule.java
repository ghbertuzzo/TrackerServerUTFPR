/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package trackerserverutfpr;

/**
 *
 * @author giova
 */
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

