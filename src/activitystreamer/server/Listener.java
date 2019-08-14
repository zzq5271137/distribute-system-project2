package activitystreamer.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;

public class Listener extends Thread {
    private static final Logger log = LogManager.getLogger();
    private ServerSocket serverSocket = null;
    private boolean term = false;
    private int portnum;
    
    public Listener() throws IOException {

        // keep our own copy in case it changes later
        portnum = Settings.getLocalPort();

        serverSocket = new ServerSocket(portnum);
        start();
    }
    
    public void run() {
        log.info("listening for new connections on " + portnum);
        while (!term) {
            Socket clientSocket;
            try {
                clientSocket = serverSocket.accept();
                Control.getInstance().incomingConnection(clientSocket);
            } catch (IOException e) {
                log.info("received exception, shutting down");
                setTerm(true);
            }
        }
    }

    public void setTerm(boolean term) {
        this.term = term;
        if (term)
            interrupt();
    }

}
