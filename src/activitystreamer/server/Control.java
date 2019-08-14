/**
 * This class serve a server to monitor and save all parameters in a server.
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

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import activitystreamer.util.Settings;

import activitystreamer.server.MsgBuff;

public class Control extends Thread {
    private static final Logger log = LogManager.getLogger();
    private static ArrayList<Connection> connections;
    private static boolean term = false;
    private static Listener listener;
    private static String serverID;

    /*
     * A list contains data(load, address, so on) of all servers who has connected
     * to this network.
     */
    private static ArrayList<Map<String, String>> interconnectedServers;

    /*
     * A list buffers data retrieved from server announces received from all other
     * servers. The data in "interconnectedServers" are refreshed by the data in
     * this buffer every several seconds (when this server "do Activity", in method
     * doActivity()).
     * 
     * We do this because, when a server crashed, we want every other servers to
     * know it (by deleting this crashed server's data from the list). But, when a
     * server crashed, only the server who is connected to it directly could know
     * it. So, every time a server receives an announcement from another server, he
     * will buffer these data in the announce in a buffer list. And Every several
     * seconds, this server will clear up the data in the "interconnectedServers"
     * and fill it with the data in the buffer list. By doing this, the data of the
     * crashed server will gone.
     */
    private static ArrayList<Map<String, String>> interconnectedServersBuff;

    // a list containing addresses of the servers authenticated by this server
    private static ArrayList<String> authenticatedServers; // <socketAddress>

    // a map containing information of all registered clients
    private static Map<String, String> registeredClients; // <username, secret>

    /*
     * a map containing information and the connection who is trying to register
     * (does not successfully register yet)
     */
    private static Map<String, Connection> registeringClients; // <username, con>

    /*
     * a map containing information and addresses of clients who has logged in on
     * THIS server
     */
    private static Map<String, String> loggedinClients; // <username:secret, socketAddress>

    /*
     * a list containing addresses of clients who has logged in on this server
     * anonymously
     */
    private static ArrayList<String> loggedinAnonymous; // <socketAddress>

    /*
     * a map containing information and the connection who is trying to logging in
     * on this server(does not successfully log in yet)
     */
    private static Map<String, Connection> loggingClients; // <username, con>

    // a map containing information and the logged out time of clients
    private static Map<String, Long> loggedOutClients; // <username:secret, loggedOutTime>

    // a map containing all broadcast message
    private static Map<Long, JSONObject> allBroadcastMsg; // <msgTime, broadcastMsg>

    // a map containing all buffers, each buffer contains messages for each clients
    private static Map<String, MsgBuff> msgBuffMap; // msg_in_order <username, msgBuff>

    /*
     * The hostname and port number of another server. These information are
     * designed to sent to other servers when they are successfully authenticated by
     * this server. When this server crashed, other serves who connect to this
     * server will use these information to try to connect to another server
     * indicated by this hostname and port number.
     */
    private static String backupSvNameToSend = null;
    private static int backupSvPortToSend = 0;

    /*
     * The hostname and port number of another server. These information are
     * designed to be the backup when this server detect a disconnection to another
     * server.
     */
    private static String backupSvNameToUse = null;
    private static int backupSvPortToUse = 0;

    private static boolean isRootServer = false;

    protected static Control control = null;

    public static Control getInstance() {
        if (control == null) {
            control = new Control();
        }
        return control;
    }

    public Control() {
        // initialize the connections array
        connections = new ArrayList<Connection>();
        // initialize the interconnected-servers list
        interconnectedServers = new ArrayList<Map<String, String>>();
        // initialize the buffer list
        interconnectedServersBuff = new ArrayList<Map<String, String>>();
        // initialize the serverID
        serverID = Settings.nextSecret();
        // initialize the authenticated-servers list
        authenticatedServers = new ArrayList<String>();
        // initialize the registered-clients map
        registeredClients = new HashMap<String, String>();
        // initialize the registering-clients map
        registeringClients = new HashMap<String, Connection>();
        // initialize the map for saving loggedin clients with username
        loggedinClients = new HashMap<String, String>();
        // initialize the list for saving anonymous loggedin clients
        loggedinAnonymous = new ArrayList<String>();
        // initialize the map for saving clients who are logging in
        loggingClients = new HashMap<String, Connection>();
        // initialize the map for saving logged out clients
        loggedOutClients = new HashMap<String, Long>();
        // initialize the map for saving all broadcast messages
        allBroadcastMsg = new HashMap<Long, JSONObject>();
        msgBuffMap = new HashMap<String, MsgBuff>();
        // new initial backup and upper server address;
        if (Settings.getRemoteHostname() == null) {
            isRootServer = true;
        }

        // start a listener
        try {
            listener = new Listener();
        } catch (IOException e1) {
            log.fatal("failed to startup a listening thread: " + e1);
            System.exit(-1);
        }
        initiateConnection();
        start();
    }

    public void initiateConnection() {
        // make a connection to another server if remote hostname is supplied
        if (Settings.getRemoteHostname() != null
                && ControlSolution.hasSecret()) {
            try {
                outgoingConnection(new Socket(Settings.getRemoteHostname(),
                        Settings.getRemotePort()));
            } catch (IOException e) {
                log.error("failed to make connection to "
                        + Settings.getRemoteHostname() + ":"
                        + Settings.getRemotePort() + " :" + e);
                System.exit(-1);
            }
        } else if (Settings.getRemoteHostname() == null
                && ControlSolution.hasSecret()) {
            /*
             * if a remote hostname is not supplied but a secret is, it means this is the
             * first server, so the supplied secret is the customised server secret, then
             * save it as the server secret
             */
            ControlSolution.setAuthenSecret(Settings.getSecret());
        } else if (!ControlSolution.hasSecret()) {
            // to start a server, a secret must be supplied
            log.error("need a secret to start a server");
            System.exit(-1);
        }
    }

    /*
     * Processing incoming messages from the connection. Return true if the
     * connection should close.
     */
    public synchronized boolean process(Connection con, String msg) {
        switch (ControlSolution.getCommandName(con, msg)) {
        case "AUTHENTICATE":
            con.setServer(true);
            return ControlSolution.receiveAuthenticate(con, msg);
        case "AUTHENTICATION_FAIL":
            return ControlSolution.receiveAuthenticationFail(con, msg);
        case "SERVER_ANNOUNCE":
            return ControlSolution.receiveServerAnnounce(con, msg);
        case "REGISTER":
            return ControlSolution.receiveRegister(msg, con);
        case "LOCK_REQUEST":
            return ControlSolution.receiveLockRequest(msg, con);
        case "LOCK_DENIED":
            return ControlSolution.receiveLockDenied(msg, con);
        case "LOCK_ALLOWED":
            return ControlSolution.receiveLockAllowed(msg, con);
        case "LOGIN":
            return ControlSolution.receiveLogin(msg, con);
        case "LOGOUT":
            return ControlSolution.receiveLogout(con);
        case "ACTIVITY_BROADCAST":
            return ControlSolution.receiveActivityBroadcast(con, msg);
        case "ACTIVITY_MESSAGE":
            return ControlSolution.receiveActivityMessage(con, msg);
        case "INVALID_MESSAGE":
            return ControlSolution.receiveInvalidMessage(con, msg);
        case "ANNOUNCE_LOGIN":
            return ControlSolution.receiveAnnounceLogin(msg, con);
        case "LOGIN_LOCK":
            return ControlSolution.receiveLoginLock(con, msg);
        case "LOGIN_ALLOWED":
            return ControlSolution.receiveLoginAllowed(con, msg);
        case "LOGIN_DENIED":
            return ControlSolution.receiveLoginDenied(con, msg);
        case "BACKUP_SERVER":
            return ControlSolution.receiveBackupServer(con, msg);
        case "AUTHENTICATION_SUCCESS":
            return ControlSolution.receiveAuthenticationSucc(con, msg);
        case "LAST_LOGOUT":
            return ControlSolution.receiveLastLogout(con, msg);
        case "": // received message do not have command field
            return true;
        default: // unknown command
            String response = ControlSolution.sendInvalidMessage(
                    "received message contains unknown command");
            con.writeMsg(response);
            return true;
        }
    }

    /*
     * A new incoming connection has been established, and a reference is returned
     * to it
     */
    public synchronized Connection incomingConnection(Socket s)
            throws IOException {
        log.debug("incomming connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s);
        connections.add(c);
        return c;
    }

    /*
     * A new outgoing connection has been established, and a reference is returned
     * to it
     */
    public synchronized Connection outgoingConnection(Socket s)
            throws IOException {
        log.debug("outgoing connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s);
        c.writeMsg(ControlSolution.sendAuthenticate());

        // this connection is a connection between two servers
        c.setServer(true);

        /*
         * this connection is a connection created by a server in order to connect to
         * another server
         */
        c.setSvOutgoingForSv();

        connections.add(c);
        addAuthenServer(Settings.socketAddress(s));
        return c;
    }

    public void run() {
        log.info("using activity interval of " + Settings.getActivityInterval()
                + " milliseconds");
        while (!term) {
            // do something with 5 second intervals in between
            try {
                Thread.sleep(Settings.getActivityInterval());
            } catch (InterruptedException e) {
                log.info("received an interrupt, system is shutting down");
                break;
            }
            if (!term) {
                log.debug("doing activity");
                term = doActivity();
            }
        }
        log.info("closing " + connections.size() + " connections");
        // clean up
        for (Connection connection : connections) {
            connection.closeCon();
        }
        listener.setTerm(true);
    }

    @SuppressWarnings("unchecked")
    public boolean doActivity() {
        for (Connection c : connections) {
            if (c.isServer())
                c.writeMsg(ControlSolution.sendServerAnnounce());
        }

        /*
         * Randomly redirect a client to another server if the load of this server is
         * high.To avoid the situation that, a client is redirected too frequently, we
         * use a chance(not 100 percent that at this tick, this server will redirect a
         * client, it may choose not to do it)
         */
        if (ControlSolution.getTheChance(Settings.getRedirectChance())) {
            for (Connection c : connections) {
                if (!c.isServer()) {
                    ControlSolution.sendRedirect(c);
                    break;
                }
            }
        }

        /*
         * refresh the data in the "interconnectedServers" by extracting data from the
         * buffer list.
         */
        interconnectedServers.clear();
        interconnectedServers = (ArrayList<Map<String, String>>) interconnectedServersBuff
                .clone();
        interconnectedServersBuff.clear();

        log.debug("**Redirect Info**");
        log.debug("----backup address to send: " + backupSvNameToSend + ":"
                + backupSvPortToSend);
        log.debug("----backup address to use: " + backupSvNameToUse + ":"
                + backupSvPortToUse);
        return false;
    }

    /**
     * Initial server crash callback method.
     */
    public void crashRedirect() {
        try { // try to reconnect to original upper server
            outgoingConnection(
                    new Socket(backupSvNameToSend, backupSvPortToSend));
        } catch (IOException eOrigin) {
            log.error("failed to reconnect to original server "
                    + backupSvNameToSend + ":" + backupSvPortToSend + " :"
                    + eOrigin);
            setBackupSvNameToSend(null);
            setBackupSvPortToSend(0);
            try { // original server unaccessible, try backup server
                if (backupSvNameToUse != null // have backup server
                        && backupSvPortToUse != 0) {
                    Settings.setRemoteHostname(backupSvNameToUse);
                    Settings.setRemotePort(backupSvPortToUse);
                    outgoingConnection(
                            new Socket(backupSvNameToUse, backupSvPortToUse));
                    setBackupSvNameToUse(null);
                    setBackupSvPortToUse(0);
                } else {
                    isRootServer = true;
                    chooseBackupSvToSend();
                    log.warn(
                            "Upper server crashed, becoming root server with no outgoing connection");
                }
            } catch (IOException eBackup) {
                log.error("failed to reconnect to backup server "
                        + backupSvNameToUse + ":" + backupSvPortToUse + " :"
                        + eBackup);
                System.exit(-1);
            }
        }
    }

    /**
     * For the root server to choose a back up server which is used to send to other
     * servers who connect to this server.
     */
    public void chooseBackupSvToSend() { // only for root server to find a upper server
        if (!isRootServer) {
            return;
        }
        for (Connection c : connections) {
            if (c.isServer()) {
                setBackupSvNameToSend(c.getConnectingSvName());
                setBackupSvPortToSend(c.getConnectingSvPort());
            }
            ControlSolution.broadcastWithinServers(c,
                    ControlSolution.sendBackupServer(), false);
            return;
        }
        setBackupSvNameToSend(null);
        setBackupSvPortToSend(0);
        setBackupSvNameToUse(null);
        setBackupSvPortToUse(0);
        log.debug(
                "no proper server to be upper server, waiting for incoming server");
    }

    /*
     * The connection has been closed by the other party.
     */
    public synchronized void connectionClosed(Connection con) {
        if (!term)
            connections.remove(con);
    }

    public synchronized void addAuthenServer(String serverSendingAddress) {
        if (!term)
            authenticatedServers.add(serverSendingAddress);
    }

    public synchronized void removeAuthenServer(String serverSendingAddress) {
        if (!term)
            authenticatedServers.remove(serverSendingAddress);
    }

    public synchronized void addConnnectedServerBuff(
            Map<String, String> serverState) {
        if (!term)
            interconnectedServersBuff.add(serverState);
    }

    public synchronized void removeConnectedServerBuff(
            Map<String, String> serverState) {
        if (!term)
            interconnectedServersBuff.remove(serverState);
    }

    public synchronized void addRegisteringClient(String username,
            Connection con) {
        if (!term)
            registeringClients.put(username, con);
    }

    public synchronized void removeRegisteringClient(String username) {
        if (!term)
            registeringClients.remove(username);
    }

    public synchronized void addRegisteredClient(String username,
            String secret) {
        if (!term)
            registeredClients.put(username, secret);
    }

    public synchronized void removeRegisteredClient(String username) {
        if (!term)
            registeredClients.remove(username);
    }

    public synchronized void addLoggedinClient(String usernameSecret,
            String socketAddress) {
        if (!term)
            loggedinClients.put(usernameSecret, socketAddress);
    }

    public synchronized void removeLoggedinClient(String usernameSecret) {
        if (!term)
            loggedinClients.remove(usernameSecret);
    }

    public synchronized void addLoggedinAnonymous(String socketAddress) {
        if (!term)
            loggedinAnonymous.add(socketAddress);
    }

    public synchronized void removeLoggedinAnonymous(String socketAddress) {
        if (!term)
            loggedinAnonymous.remove(socketAddress);
    }

    public synchronized void addLoggingClient(String username,
            Connection con) {
        if (!term)
            loggingClients.put(username, con);
    }

    public synchronized void removeLoggingClient(String username) {
        if (!term)
            loggingClients.remove(username);
    }

    public synchronized void addLoggedOutClient(String usernameSecret,
            Long loggedOutTime) {
        if (!term)
            loggedOutClients.put(usernameSecret, loggedOutTime);
    }

    public synchronized void removeLoggedOutClient(String usernameSecret) {
        if (!term)
            loggedOutClients.remove(usernameSecret);
    }

    public synchronized void addBroadcastMsg(Long msgTime, JSONObject msg) {
        if (!term)
            allBroadcastMsg.put(msgTime, msg);
    }

    public synchronized void removeMsgBuff(String clientAddr) {
        if (!term)
            msgBuffMap.remove(clientAddr);
    }

    public synchronized void addMsgBuff(String clientAddr, int order) {
        if (!term)
            msgBuffMap.put(clientAddr, new MsgBuff(order));
    }

    public final void setTerm(boolean t) {
        term = t;
    }

    public final ArrayList<Connection> getConnections() {
        return connections;
    }

    public final ArrayList<Map<String, String>> getInterconnectedServers() {
        return interconnectedServers;
    }

    public final ArrayList<Map<String, String>> getInterconnectedServersBuff() {
        return interconnectedServersBuff;
    }

    public final ArrayList<String> getAuthenticatedServers() {
        return authenticatedServers;
    }

    public final Map<String, String> getRegisteredClients() {
        return registeredClients;
    }

    public final Map<String, Connection> getRegisteringClients() {
        return registeringClients;
    }

    public final Map<String, String> getLoggedinClients() {
        return loggedinClients;
    }

    public final ArrayList<String> getLoggedinAnonymous() {
        return loggedinAnonymous;
    }

    public final String getServerID() {
        return serverID;
    }

    public final Map<String, Connection> getLoggingClients() {
        return loggingClients;
    }

    public final Map<String, Long> getLoggedOutClients() {
        return loggedOutClients;
    }

    public final Map<String, MsgBuff> getMsgBuffMap() {
        return msgBuffMap;
    }

    public final Map<Long, JSONObject> getAllBroadcastMsg() {
        return allBroadcastMsg;
    }

    public final void setBackupSvNameToSend(String hostName) {
        backupSvNameToSend = hostName;
    }

    public final String getBackupSvNameToSend() {
        return backupSvNameToSend;
    }

    public final void setBackupSvPortToSend(int portNum) {
        backupSvPortToSend = portNum;
    }

    public final int getBackupSvPortToSend() {
        return backupSvPortToSend;
    }

    public final void setBackupSvNameToUse(String hostName) {
        backupSvNameToUse = hostName;
    }

    public final String getBackupSvNameToUse() {
        return backupSvNameToUse;
    }

    public final void setBackupSvPortToUse(int portNum) {
        backupSvPortToUse = portNum;
    }

    public final int getBackupSvPortToUse() {
        return backupSvPortToUse;
    }

}
