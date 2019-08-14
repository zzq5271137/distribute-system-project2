/**
 * 
 * @author modified by
 *         group666:
 *         Shujing Xiao
 *             (Login name: shujingx Email: shujingx@student.unimelb.edu.au)
 *         Ziyi Xiong
 *             (Login name: zxiong1 Email: zxiong1@student.unimelb.edu.au)
 *         Ziyi Xiong
 *             (Login name: zxiong1 Email: zxiong1@student.unimelb.edu.au)
 *         Zhengqing Zhu
 *             (Login name: zhengqingz Email: zhengqingz@student.unimelb.edu.au)
 */

package activitystreamer.server;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;

public class Connection extends Thread {
    private static final Logger log = LogManager.getLogger();
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private BufferedReader inreader;
    private PrintWriter outwriter;
    private boolean open = false;
    private boolean term = false;

    // the time when this connection is created
    private final long establishTime;

    // indicate whether this connection belongs to a server
    private boolean isServer = false;

    // indicate how many lock allowed has received
    private int nRequestAllo;

    // indicate how many login denied has received
    private int nLoginDenied;

    /*
     * indicate whether this connection is created by a server in order to connect
     * to another server (the outgoing connection for a server)
     */
    private boolean isSvOutgoingForSv = false;

    /*
     * The address of the opposite sever. (only useful when this connection is
     * between two servers)
     */
    private String connectingSvName = null;
    private int connectingSvPort = 0;

    Connection(Socket socket) throws IOException {
        this.socket = socket;
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        inreader = new BufferedReader(new InputStreamReader(in));
        outwriter = new PrintWriter(out, true);
        establishTime = (new Date()).getTime();
        open = true;
        start();
    }

    public String getConnectingSvName() {
        return connectingSvName;
    }

    public void setConnectingSvName(String connectingSvName) {
        this.connectingSvName = connectingSvName;
    }

    public int getConnectingSvPort() {
        return connectingSvPort;
    }

    public void setConnectingSvPort(int connectingSvPort) {
        this.connectingSvPort = connectingSvPort;
    }

    public void setSvOutgoingForSv() {
        isSvOutgoingForSv = true;
    }

    public int getNLoginDenied() {
        return nLoginDenied;
    }

    public void setNLoginDenied(int nLoginDenied) {
        this.nLoginDenied = nLoginDenied;
    }

    public void incrementNLoginDenied() {
        this.nLoginDenied++;
    }

    public int getNRequestAllo() {
        return nRequestAllo;
    }

    public void setNRequestAllo(int nRequestAllo) {
        this.nRequestAllo = nRequestAllo;
    }

    public void incrementNRequestAllo() {
        this.nRequestAllo++;
    }

    public boolean isServer() {
        return isServer;
    }

    public void setServer(boolean isServer) {
        this.isServer = isServer;
    }

    /*
     * returns true if the message was written, otherwise false
     */
    public boolean writeMsg(String msg) {
        if (open) {
            outwriter.println(msg);
            outwriter.flush();
            return true;
        }
        return false;
    }

    /*
     * close this connection
     */
    public void closeCon() {
        if (open) {
            log.info("closing connection " + Settings.socketAddress(socket));
            try {
                term = true;
                socket.close();
            } catch (IOException e) {
                // already closed?
                log.error("received exception closing the connection "
                        + Settings.socketAddress(socket) + ": " + e);
            }
        }
    }

    public void run() {
        try {
            String data;
            while (!term && (data = inreader.readLine()) != null) {
                term = Control.getInstance().process(this, data);
            }
            log.debug(
                    "connection closed to " + Settings.socketAddress(socket));
            Control.getInstance().connectionClosed(this);
            if (!socket.isClosed()) {
                socket.close();
            }
            if (isSvOutgoingForSv) {
                Control.getInstance().crashRedirect();
            } else if (isServer && this.connectingSvName.equals(
                    (String) Control.getInstance().getBackupSvNameToSend())
                    && this.connectingSvPort == Control.getInstance()
                            .getBackupSvPortToSend()) {
                Control.getInstance().chooseBackupSvToSend();
            }
            if (!this.isServer) {
                ControlSolution.receiveLogout(this);
            }
        } catch (IOException e) {
            log.error("connection " + Settings.socketAddress(socket)
                    + " closed with exception: " + e);
            Control.getInstance().connectionClosed(this);

            /*
             * Handle the server crash or connection between servers is broken. The strategy
             * we choose is that: when a connection exception is detected, if this
             * connection is a connection created by a server in order to connect to
             * another, it will try to do something (see details in method "crashRedirect()"
             * in Class "Control"). If this connection (who detects the connection
             * exception) is created by a server who accept an incoming connection from
             * other server, it will do nothing except waiting for that server to reconnect
             * to it.
             */
            if (isSvOutgoingForSv) {
                Control.getInstance().crashRedirect();
            } else if (isServer && this.connectingSvName.equals(
                    (String) Control.getInstance().getBackupSvNameToSend())
                    && this.connectingSvPort == Control.getInstance()
                            .getBackupSvPortToSend()) {
                Control.getInstance().chooseBackupSvToSend();
            }
            if (!isServer) {
                ControlSolution.receiveLogout(this);
            }
        }
        open = false;
    }

    public long getEstablishTime() {
        return establishTime;
    }

    public Socket getSocket() {
        return socket;
    }

    public boolean isOpen() {
        return open;
    }

}
