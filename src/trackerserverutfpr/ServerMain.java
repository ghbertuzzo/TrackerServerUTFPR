/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package trackerserverutfpr;

import java.io.IOException;

/**
 *
 * @author giova
 */
public class ServerMain {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException{
        // TODO code application logic here
        int httpPortNumber = 19880;
        ServerFull socketServer = new ServerFull(httpPortNumber);
        socketServer.start();
    }
    
}
