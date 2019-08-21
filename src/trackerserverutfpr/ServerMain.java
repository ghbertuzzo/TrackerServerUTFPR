package trackerserverutfpr;

import java.io.IOException;

public class ServerMain {

    public static void main(String[] args) throws IOException{
        // TODO code application logic here
        int httpPortNumber = 19880;
        ServerFull socketServer = new ServerFull(httpPortNumber);
        socketServer.start();
    }
    
}
