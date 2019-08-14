/**
 * This class handles all receiving and sending procedures for servers.
 * 
 * @author group666:
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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class ControlSolution {

    // the secret needed for a server to connect to our network
    private static String authenSecret;
    private static JSONParser parser = new JSONParser();
    private static final Logger log = LogManager.getLogger();
    private static Random random = new Random();

    /*
     * The preset percentage(chance) to redirect a client to a server with max load,
     * aims to evenly distribute clients over servers as much as we can.
     */
    private static final int REDIRECT_TO_MAX_LOAD_SERVER = 77;

    public static void setAuthenSecret(String secret) {
        authenSecret = secret;
    }

    /*
     * For all methods who are capable of receiving messages, they first check the
     * integrity of that message (does it contain certain fields and does each field
     * contain values), then they do corresponding processing actions.
     * 
     * For all methods who are capable of sending messages, they don't actually send
     * it. They just build a corresponding JSON Object and return a String of that
     * Object (pass the sending responsibility to the caller function)
     */

    /**
     * Version 2: Generate authentication to another server when start up a new
     * server. For this new version, add the host name and the listening port number
     * in the authentication message.
     */
    @SuppressWarnings("unchecked")
    public static String sendAuthenticate() {
        JSONObject authenticate = new JSONObject();
        authenticate.put("command", "AUTHENTICATE");
        authenticate.put("secret", Settings.getSecret());
        authenticate.put("hostname", Settings.getLocalHostname());
        authenticate.put("port", Integer.toString(Settings.getLocalPort()));
        return authenticate.toJSONString();
    }

    /**
     * Version 2: When receive an authentication, handle the message. For this new
     * version, when the secret is correct, reply an "AUTHENTICATION_SUCCESS"
     * message back, in order to let the opposite server (who send the
     * authentication) to do some additional work (please see method
     * "receiveAuthenticationSucc()"); What's more, if the recipient is the first
     * server (does not have "backupSvNameToSend"), save the server host name and
     * port number of the "authenticate" sender as the "backupSvNameToSend" and
     * "backupSvPortToSend".
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveAuthenticate(Connection con, String msg) {
        log.debug("received an AUTHENTICATE from "
                + Settings.socketAddress(con.getSocket()));
        JSONObject authenticate = getJSON(con, msg);
        if (authenticate == null) {
            return true;
        }
        if (!hasValidKV("secret", authenticate, con)
                || !hasValidKV("hostname", authenticate, con)
                || !hasValidKV("port", authenticate, con)) {
            return true;
        }
        String response;
        if (alreadyAuthenticated(con)) {
            response = sendInvalidMessage(
                    "the server had already successfully authenticated");
            con.writeMsg(response);
            return true;
        }
        String secret = (String) authenticate.get("secret");
        if (!secret.equals(authenSecret)) {
            response = sendAuthenticationFail(
                    "the supplied secret is incorrect: " + secret);
            con.writeMsg(response);
            return true;
        }
        Control.getInstance()
                .addAuthenServer(Settings.socketAddress(con.getSocket()));

        String hostname = (String) authenticate.get("hostname");
        int port = Integer.parseInt((String) authenticate.get("port"));
        con.setConnectingSvName(hostname);
        con.setConnectingSvPort(port);
        if (Control.getInstance().getBackupSvNameToSend() == null) {
            Control.getInstance().setBackupSvNameToSend(hostname);
            Control.getInstance().setBackupSvPortToSend(port);
        }
        response = sendAuthenticationSucc();
        con.writeMsg(response);

        return false;
    }

    /**
     * check whether a server has already authenticated or not
     * 
     * @return true if it is already authenticated
     */
    private static boolean alreadyAuthenticated(Connection con) {
        ArrayList<String> authenticatedServers = Control.getInstance()
                .getAuthenticatedServers();
        String serverSendingAddress = Settings.socketAddress(con.getSocket());
        for (String authedServ : authenticatedServers)
            if (serverSendingAddress.equals(authedServ)) {
                Control.getInstance().removeAuthenServer(serverSendingAddress);
                return true;
            }
        return false;
    }

    /**
     * generate authentication-fail message
     * 
     * @param info
     *            indicates why fail
     */
    @SuppressWarnings("unchecked")
    private static String sendAuthenticationFail(String info) {
        JSONObject authenticationFail = new JSONObject();
        authenticationFail.put("command", "AUTHENTICATION_FAIL");
        authenticationFail.put("info", info);
        return authenticationFail.toJSONString();
    }

    /**
     * when receive an authentication fail message, handle it and close the
     * connection
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveAuthenticationFail(Connection con,
            String msg) {
        log.debug("received an AUTHENTICATION_FAIL from "
                + Settings.socketAddress(con.getSocket()));
        JSONObject authenFail = getJSON(con, msg);
        if (authenFail == null) {
            return true;
        }
        if (!hasValidKV("info", authenFail, con)) {
            return true;
        }
        log.debug("info: " + authenFail.get("info"));
        String receiveFrom = Settings.socketAddress(con.getSocket());
        if (Control.getInstance().getAuthenticatedServers()
                .contains(receiveFrom)) {
            Control.getInstance().removeAuthenServer(receiveFrom);
        }
        return true;
    }

    /**
     * generate an invalid message when something wrong happens
     * 
     * @param info
     *            indicates what kind of violation it happens
     */
    @SuppressWarnings("unchecked")
    public static String sendInvalidMessage(String info) {
        JSONObject invalidMessage = new JSONObject();
        invalidMessage.put("command", "INVALID_MESSAGE");
        invalidMessage.put("info", info);
        return invalidMessage.toJSONString();
    }

    /**
     * when receive an invalid message, handle it and close the connection
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveInvalidMessage(Connection con, String msg) {
        log.debug("received an INVALID_MESSAGE from "
                + Settings.socketAddress(con.getSocket()));
        JSONObject invalidMsg = getJSON(con, msg);
        if (invalidMsg == null) {
            return true;
        }
        if (!hasValidKV("info", invalidMsg, con)) {
            return true;
        }
        log.debug("info: " + invalidMsg.get("info"));
        return true;
    }

    /**
     * a message indicating who am I, where I am listening, how many clients I have
     * (I am a server)
     */
    @SuppressWarnings("unchecked")
    public static String sendServerAnnounce() {
        JSONObject severAnnon = new JSONObject();
        severAnnon.put("command", "SERVER_ANNOUNCE");
        severAnnon.put("id", Control.getInstance().getServerID());
        severAnnon.put("load",
                localLoad(Control.getInstance().getConnections()));
        severAnnon.put("hostname", Settings.getLocalHostname());
        severAnnon.put("port", Settings.getLocalPort());
        return severAnnon.toJSONString();
    }

    /**
     * when receive a server announce, handle it, forward it
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveServerAnnounce(Connection con, String msg) {
        log.debug("received a SERVER_ANNOUNCE from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject severAnnon = getJSON(con, msg);
        if (severAnnon == null) {
            return true;
        }
        if (!hasValidKV("id", severAnnon, con)
                || !hasValidKV("hostname", severAnnon, con)) {
            return true;
        }
        if (notContainsField("load", severAnnon, con)
                || notContainsField("port", severAnnon, con)) {
            return true;
        }
        log.debug("received an announcement from " + severAnnon.get("id")
                + " load " + severAnnon.get("load") + " at "
                + severAnnon.get("hostname") + ":" + severAnnon.get("port"));
        updateSeverStates(severAnnon);
        broadcastWithinServers(con, msg, true);
        return false;
    }

    /**
     * Version 2: Update the list where containing all information about all servers
     * which connect to the whole network.For this new version, add information
     * about servers into the buffer list first. And copy this list to the
     * server-state list (please see comments for "interconnectedServersBuff" in
     * class "Control").
     */
    private static void updateSeverStates(JSONObject severAnnon) {
        ArrayList<Map<String, String>> interconnectedServersBuff = Control
                .getInstance().getInterconnectedServersBuff();
        for (Map<String, String> serverState : interconnectedServersBuff) {
            if (serverState.get("id").equals((String) severAnnon.get("id"))) {
                Control.getInstance().removeConnectedServerBuff(serverState);
                break;
            }
        }
        Map<String, String> serverState = new HashMap<String, String>();
        serverState.put("id", (String) severAnnon.get("id"));
        serverState.put("load", severAnnon.get("load").toString());
        serverState.put("hostname", (String) severAnnon.get("hostname"));
        serverState.put("port", severAnnon.get("port").toString());
        Control.getInstance().addConnnectedServerBuff(serverState);
    }

    /**
     * Version 2: When receive a login message from a client, handle it.
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveLogin(String msg, Connection con) {
        log.debug("received a LOGIN from "
                + Settings.socketAddress(con.getSocket()));
        JSONObject login = getJSON(con, msg);
        if (login == null) {
            return true;
        }
        if (!hasValidKV("username", login, con)) {
            return true;
        }
        String username = (String) login.get("username");
        String response;
        if (username.equals("anonymous")) {
            response = sendLoginSucc("anonymous");
            con.writeMsg(response);
            if (!sendRedirect(con)) {
                Control.getInstance().addLoggedinAnonymous(
                        Settings.socketAddress(con.getSocket()));
            }
            return false;
        } else {
            if (!hasValidKV("secret", login, con)) {
                return true;
            }
            String secret = (String) login.get("secret");
            Map<String, String> registeredClients = Control.getInstance()
                    .getRegisteredClients();
            if (!registeredClients.containsKey(username)) {
                if (Control.getInstance().getInterconnectedServers()
                        .size() == 0) {
                    response = sendLoginFail(
                            "attempt to login with unregistered username");
                    con.writeMsg(response);
                    return true;
                }
                con.setNLoginDenied(0);
                Control.getInstance().addLoggingClient(username, con);
                String loginLock = sendLoginLock(username, secret);
                broadcastWithinServers(con, loginLock, false);
                return false;
            }
            if (!secret.equals(registeredClients.get(username))) {
                response = sendLoginFail("attempt to login with wrong secret");
                con.writeMsg(response);
                return true;
            }
            String identifier = username + ":" + secret;
            kickOutClient(identifier);
            response = sendLoginSucc(username);
            con.writeMsg(response);
            String loginAllowed = sendLoginAllowed(username, secret);
            broadcastWithinServers(con, loginAllowed, false);
            if (!sendRedirect(con)) {
                Control.getInstance().addLoggedinClient(identifier,
                        Settings.socketAddress(con.getSocket()));
                if (Control.getInstance().getLoggedOutClients()
                        .containsKey(identifier)) {
                    sendHistoryMsgToClients(identifier, Control.getInstance()
                            .getLoggedOutClients().get(identifier));
                }
                String announceLogin = sendAnnounceLogin(username, secret);
                broadcastWithinServers(con, announceLogin, false);
            }
            return false;
        }
    }

    /**
     * Newly added message.
     */
    @SuppressWarnings("unchecked")
    private static String sendLoginLock(String username, String secret) {
        JSONObject loginLock = new JSONObject();
        loginLock.put("command", "LOGIN_LOCK");
        loginLock.put("username", username);
        loginLock.put("secret", secret);
        return loginLock.toJSONString();
    }

    /**
     * New method.
     */
    public static boolean receiveLoginLock(Connection con, String msg) {
        log.debug("received a LOGIN_LOCK from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject loginLock = getJSON(con, msg);
        if (loginLock == null) {
            return true;
        }
        if (!hasValidKV("username", loginLock, con)
                || !hasValidKV("secret", loginLock, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);
        String username = (String) loginLock.get("username");
        String secret = (String) loginLock.get("secret");
        String response;
        Map<String, String> registeredClients = Control.getInstance()
                .getRegisteredClients();
        if (registeredClients.containsKey(username)
                && secret.equals(registeredClients.get(username))) {
            response = sendLoginAllowed(username, secret);
            broadcastWithinServers(con, response, false);
            return false;
        }
        response = sendLoginDenied(username);
        broadcastWithinServers(con, response, false);
        return false;
    }

    /**
     * Newly added message.
     */
    @SuppressWarnings("unchecked")
    private static String sendLoginAllowed(String username, String secret) {
        JSONObject loginAllowed = new JSONObject();
        loginAllowed.put("command", "LOGIN_ALLOWED");
        loginAllowed.put("username", username);
        loginAllowed.put("secret", secret);
        return loginAllowed.toJSONString();
    }

    /**
     * New method.
     */
    public static boolean receiveLoginAllowed(Connection con, String msg) {
        log.debug("received a LOGIN_ALLOWED from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject loginAllowed = getJSON(con, msg);
        if (loginAllowed == null) {
            return true;
        }
        if (!hasValidKV("username", loginAllowed, con)
                || !hasValidKV("secret", loginAllowed, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);
        String username = (String) loginAllowed.get("username");
        String secret = (String) loginAllowed.get("secret");
        if (!Control.getInstance().getRegisteredClients()
                .containsKey(username)) {
            Control.getInstance().addRegisteredClient(username, secret);
        }

        Map<String, Connection> loggingClients = Control.getInstance()
                .getLoggingClients();
        if (loggingClients.containsKey(username)) {
            Connection loggingCon = loggingClients.get(username);
            String loginSucc = sendLoginSucc(username);
            loggingCon.writeMsg(loginSucc);
            String identifier = username + ":" + secret;
            if (!sendRedirect(loggingCon)) {
                Control.getInstance().addLoggedinClient(identifier,
                        Settings.socketAddress(loggingCon.getSocket()));
                String announceLogin = sendAnnounceLogin(username, secret);
                broadcastWithinServers(con, announceLogin, false);
            }
            Control.getInstance().removeLoggingClient(username);
        }

        return false;
    }

    /**
     * Newly added message.
     */
    @SuppressWarnings("unchecked")
    private static String sendLoginDenied(String username) {
        JSONObject loginDenied = new JSONObject();
        loginDenied.put("command", "LOGIN_DENIED");
        loginDenied.put("username", username);
        return loginDenied.toJSONString();
    }

    /**
     * New method.
     */
    public static boolean receiveLoginDenied(Connection con, String msg) {
        log.debug("received a LOGIN_DENIED from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject loginDenied = getJSON(con, msg);
        if (loginDenied == null) {
            return true;
        }
        if (!hasValidKV("username", loginDenied, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);

        String username = (String) loginDenied.get("username");
        Map<String, Connection> loggingClients = Control.getInstance()
                .getLoggingClients();
        if (loggingClients.containsKey(username)) {
            Connection loggingCon = loggingClients.get(username);
            loggingCon.incrementNLoginDenied();
            int nConnectedServers = Control.getInstance()
                    .getInterconnectedServers().size();
            if (loggingCon.getNLoginDenied() == nConnectedServers) {
                String response = sendLoginFail(
                        "attempt to login with unregistered username");
                loggingCon.writeMsg(response);
                Control.getInstance().removeLoggingClient(username);
            }
        }

        return false;
    }

    /**
     * Newly added message.
     */
    @SuppressWarnings("unchecked")
    private static String sendAnnounceLogin(String username, String secret) {
        JSONObject announceLogin = new JSONObject();
        announceLogin.put("command", "ANNOUNCE_LOGIN");
        announceLogin.put("username", username);
        announceLogin.put("secret", secret);
        return announceLogin.toJSONString();
    }

    /**
     * New method.
     */
    public static boolean receiveAnnounceLogin(String msg, Connection con) {
        log.debug("received an ANNOUNCE_LOGIN from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject annoLogin = getJSON(con, msg);
        if (annoLogin == null) {
            return true;
        }
        if (!hasValidKV("username", annoLogin, con)
                || !hasValidKV("secret", annoLogin, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);
        String username = (String) annoLogin.get("username");
        String secret = (String) annoLogin.get("secret");
        String identifier = username + ":" + secret;
        kickOutClient(identifier);
        Map<String, Long> loggedOutClients = Control.getInstance()
                .getLoggedOutClients();
        if (loggedOutClients.containsKey(identifier)) {
            long loggedOutTime = loggedOutClients.get(identifier);
            String lastLogout = sendLastLogout(username, secret,
                    loggedOutTime);
            Control.getInstance().removeLoggedOutClient(identifier);
            broadcastWithinServers(con, lastLogout, false);
        }
        String clientAddr = Settings.socketAddress(con.getSocket());
        if (Control.getInstance().getMsgBuffMap().containsKey(clientAddr)) {
            Control.getInstance().removeMsgBuff(clientAddr);
        }
        return false;
    }

    /**
     * New method.
     */
    private static void kickOutClient(String identifier) {
        Map<String, String> loggedinClients = Control.getInstance()
                .getLoggedinClients();
        if (loggedinClients.containsKey(identifier)) {
            String socketAddress = loggedinClients.get(identifier);
            Control.getInstance().removeLoggedinClient(identifier);
            ArrayList<Connection> connections = Control.getInstance()
                    .getConnections();
            for (Connection connect : connections) {
                if (socketAddress
                        .equals(Settings.socketAddress(connect.getSocket()))) {
                    String authenFail = sendAuthenticationFail(
                            "you have been kicked out by another client");
                    connect.writeMsg(authenFail);
                    connect.closeCon();
                    long loggedOutTime = new Date().getTime();
                    Control.getInstance().addLoggedOutClient(identifier,
                            loggedOutTime);
                }
            }
        }
    }

    /**
     * Newly added message.
     */
    @SuppressWarnings("unchecked")
    private static String sendLastLogout(String username, String secret,
            long loggedOutTime) {
        JSONObject lastLogout = new JSONObject();
        lastLogout.put("command", "LAST_LOGOUT");
        lastLogout.put("username", username);
        lastLogout.put("secret", secret);
        lastLogout.put("logouttime", Long.toString(loggedOutTime));
        return lastLogout.toJSONString();
    }

    /**
     * New method.
     */
    public static boolean receiveLastLogout(Connection con, String msg) {
        log.debug("received an LAST_LOGOUT from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject lastLogout = getJSON(con, msg);
        if (lastLogout == null) {
            return true;
        }
        if (!hasValidKV("username", lastLogout, con)
                || !hasValidKV("secret", lastLogout, con)
                || !hasValidKV("logouttime", lastLogout, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);
        String username = (String) lastLogout.get("username");
        String secret = (String) lastLogout.get("secret");
        String identifier = username + ":" + secret;
        if (Control.getInstance().getLoggedinClients()
                .containsKey(identifier)) {
            long logouttime = Long
                    .parseLong((String) lastLogout.get("logouttime"));
            sendHistoryMsgToClients(identifier, logouttime);
        }
        return false;
    }

    /**
     * New method.
     */
    private static void sendHistoryMsgToClients(String identifier,
            long logouttime) {
        ArrayList<Connection> connections = Control.getInstance()
                .getConnections();
        String clientAddr = Control.getInstance().getLoggedinClients()
                .get(identifier);
        Connection clientCon = null;
        for (Connection c : connections) {
            if (Settings.socketAddress(c.getSocket()).equals(clientAddr)) {
                clientCon = c;
                break;
            }
        }
        if (clientCon == null) {
            log.error("something wrong, system exit");
            System.exit(-1);
        }
        Map<Long, JSONObject> allBroadcastMsg = Control.getInstance()
                .getAllBroadcastMsg();
        for (long msgtime : allBroadcastMsg.keySet()) {
            if ((logouttime - msgtime) <= Settings.LATENCY) {
                String historyMsg = allBroadcastMsg.get(msgtime)
                        .toJSONString();
                clientCon.writeMsg(historyMsg);
            }
        }
    }

    /**
     * Version 2
     * 
     * @param con
     *            to whom to send the message
     * @return whether send the redirect message or not
     */
    public static boolean sendRedirect(Connection con) {
        String redirectMsg = generateRedirectMsg();
        if (!redirectMsg.equals("false")) {
            con.writeMsg(redirectMsg);
            return true;
        }
        return false;
    }

    /**
     * New method.
     */
    @SuppressWarnings("unchecked")
    public static String generateRedirectMsg() {
        ArrayList<Map<String, String>> candidate = new ArrayList<Map<String, String>>();
        boolean isRedir = false;
        ArrayList<Connection> connections = Control.getInstance()
                .getConnections();
        int localLoad = localLoad(connections);
        ArrayList<Map<String, String>> interconnectedServers = Control
                .getInstance().getInterconnectedServers();
        for (Map<String, String> server : interconnectedServers) {
            int difference = Integer.parseInt(server.get("load")) - localLoad;
            if (difference >= 2) {
                candidate.add(server);
            }
        }
        if (!candidate.isEmpty()) {
            isRedir = true;
        }
        if (isRedir) {
            Map<String, String> objectServer = aimServer(candidate, localLoad);
            JSONObject redirInfo = new JSONObject();
            redirInfo.put("command", "REDIRECT");
            redirInfo.put("hostname", objectServer.get("hostname"));
            redirInfo.put("port", objectServer.get("port"));
            return redirInfo.toJSONString();
        }
        return "false";
    }

    /**
     * New method.
     */
    private static Map<String, String> aimServer(
            ArrayList<Map<String, String>> candidate, int localLoad) {
        ArrayList<Map<String, String>> maxLoadServers = new ArrayList<Map<String, String>>();
        Map<String, String> objectServer = null;
        int maxDifference = 0;

        for (Map<String, String> server : candidate) {
            int difference = Integer.parseInt(server.get("load")) - localLoad;
            if (difference > maxDifference) {
                maxDifference = difference;
            }
        }

        for (Map<String, String> server : candidate) {
            int difference = Integer.parseInt(server.get("load")) - localLoad;
            if (difference == maxDifference) {
                maxLoadServers.add(server);
            }
        }
        if (maxLoadServers.isEmpty()) {
            log.error("something wrong, system exit");
            System.exit(-1);
        }

        if (getTheChance(REDIRECT_TO_MAX_LOAD_SERVER)) {
            objectServer = maxLoadServers
                    .get(random.nextInt(maxLoadServers.size()));
        } else {
            candidate.removeAll(maxLoadServers);
            if (!candidate.isEmpty()) {
                objectServer = candidate.get(random.nextInt(candidate.size()));
            } else {
                objectServer = maxLoadServers
                        .get(random.nextInt(maxLoadServers.size()));
            }
        }

        return objectServer;
    }

    /**
     * New Method. Helper function.
     * 
     * @param presentage
     * @return whether it get the chance to do something or not
     */
    public static boolean getTheChance(int presentage) {
        return random.nextInt(100) <= presentage;
    }

    /**
     * generate a login fail message to a client
     * 
     * @param info
     *            indicate why login fail
     */
    @SuppressWarnings("unchecked")
    private static String sendLoginFail(String info) {
        JSONObject loginFail = new JSONObject();
        loginFail.put("command", "LOGIN_FAILED");
        loginFail.put("info", info);
        return loginFail.toJSONString();
    }

    /**
     * generate a login success message to a client
     */
    @SuppressWarnings("unchecked")
    private static String sendLoginSucc(String username) {
        JSONObject loginSucc = new JSONObject();
        loginSucc.put("command", "LOGIN_SUCCESS");
        loginSucc.put("info", "logged in as user " + username);
        return loginSucc.toJSONString();
    }

    /**
     * Version 2. When receive a logout message, close the connection.
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveLogout(Connection con) {
        log.debug("received a LOGOUT from "
                + Settings.socketAddress(con.getSocket()));
        Map<String, String> loggedinClients = Control.getInstance()
                .getLoggedinClients();
        String clientAddr = Settings.socketAddress(con.getSocket());
        for (String key : loggedinClients.keySet()) {
            if (clientAddr.equals(loggedinClients.get(key))) {
                Control.getInstance().removeLoggedinClient(key);
                long loggedOutTime = new Date().getTime();
                Control.getInstance().addLoggedOutClient(key, loggedOutTime);
                break;
            }
        }
        if (Control.getInstance().getLoggedinAnonymous()
                .contains(clientAddr)) {
            Control.getInstance().removeLoggedinAnonymous(clientAddr);
        }
        return true;
    }

    /**
     * when receive a register message, handle it and broadcast a lock request
     * message within servers
     * 
     * assumption: there is no such case that two clients are registering with the
     * same username to two different servers at the same time
     */
    public static boolean receiveRegister(String msg, Connection con) {
        log.debug("received a REGISTER from "
                + Settings.socketAddress(con.getSocket()));
        JSONObject register = getJSON(con, msg);
        if (register == null) {
            return true;
        }
        if (!hasValidKV("username", register, con)
                || !hasValidKV("secret", register, con)) {
            return true;
        }
        String response;
        String username = (String) register.get("username");
        String secret = (String) register.get("secret");
        Map<String, String> registeredClients = Control.getInstance()
                .getRegisteredClients();
        if (registeredClients.containsKey(username)) {
            response = sendRegisterFail(username);
            con.writeMsg(response);
            return true;
        }
        if (Control.getInstance().getInterconnectedServers().size() == 0) {
            Control.getInstance().addRegisteredClient(username, secret);
            response = sendRegisterSucc(username);
            con.writeMsg(response);
            return false;
        }
        con.setNRequestAllo(0);
        Control.getInstance().addRegisteringClient(username, con);
        String lockReq = sendLockRequest(username, secret);
        broadcastWithinServers(con, lockReq, false);
        return false;
    }

    /**
     * generate a register fail message to a client
     */
    @SuppressWarnings("unchecked")
    public static String sendRegisterFail(String username) {
        JSONObject registerFail = new JSONObject();
        registerFail.put("command", "REGISTER_FAILED");
        registerFail.put("info",
                username + " is already registered with the system");
        return registerFail.toJSONString();
    }

    /**
     * generate a register success message to a client
     */
    @SuppressWarnings("unchecked")
    public static String sendRegisterSucc(String username) {
        JSONObject registerSucc = new JSONObject();
        registerSucc.put("command", "REGISTER_SUCCESS");
        registerSucc.put("info", "register success for " + username);
        return registerSucc.toJSONString();
    }

    /**
     * generate a lock request message to indicate a user wanting to register with
     * given username and secret
     */
    @SuppressWarnings("unchecked")
    private static String sendLockRequest(String username, String secret) {
        JSONObject lockReq = new JSONObject();
        lockReq.put("command", "LOCK_REQUEST");
        lockReq.put("username", username);
        lockReq.put("secret", secret);
        return lockReq.toJSONString();
    }

    /**
     * when receive a lock request message, handle it (first check its local
     * storage, then broadcast corresponding message within servers)
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveLockRequest(String msg, Connection con) {
        log.debug("received a LOCK_REQUEST from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject lockReq = getJSON(con, msg);
        if (lockReq == null) {
            return true;
        }
        if (!hasValidKV("username", lockReq, con)
                || !hasValidKV("secret", lockReq, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);
        String response;
        String username = (String) lockReq.get("username");
        String secret = (String) lockReq.get("secret");
        Map<String, String> registeredClients = Control.getInstance()
                .getRegisteredClients();
        if (registeredClients.containsKey(username)
                && !secret.equals(registeredClients.get(username))) {
            Control.getInstance().removeRegisteredClient(username);
            response = sendLockDenied(username, secret);
            broadcastWithinServers(con, response, false);
            return false;
        }
        if (!registeredClients.containsKey(username)) {
            Control.getInstance().addRegisteredClient(username, secret);
        }
        response = sendLockAllowed(username, secret);
        broadcastWithinServers(con, response, false);
        return false;
    }

    @SuppressWarnings("unchecked")
    private static String sendLockDenied(String username, String secret) {
        JSONObject lockDen = new JSONObject();
        lockDen.put("command", "LOCK_DENIED");
        lockDen.put("username", username);
        lockDen.put("secret", secret);
        return lockDen.toJSONString();
    }

    /**
     * when receive a lock denied, the server will check whether it is himself who
     * receive the register message initially, if it is, then send a register fail
     * message to that client
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveLockDenied(String msg, Connection con) {
        log.debug("received a LOCK_DENIED from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject lockDen = getJSON(con, msg);
        if (lockDen == null) {
            return true;
        }
        if (!hasValidKV("username", lockDen, con)
                || !hasValidKV("secret", lockDen, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);
        String username = (String) lockDen.get("username");
        String secret = (String) lockDen.get("secret");
        Map<String, String> registeredClients = Control.getInstance()
                .getRegisteredClients();
        if (registeredClients.containsKey(username)
                && secret.equals(registeredClients.get(username))) {
            Control.getInstance().removeRegisteredClient(username);
        }
        Map<String, Connection> registeringClients = Control.getInstance()
                .getRegisteringClients();
        if (registeringClients.containsKey(username)) {
            Connection registeringCon = registeringClients.get(username);
            String response = sendRegisterFail(username);
            registeringCon.writeMsg(response);
            registeringCon.closeCon();
            Control.getInstance().removeRegisteringClient(username);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static String sendLockAllowed(String username, String secret) {
        JSONObject lockAllo = new JSONObject();
        lockAllo.put("command", "LOCK_ALLOWED");
        lockAllo.put("username", username);
        lockAllo.put("secret", secret);
        return lockAllo.toJSONString();
    }

    /**
     * when receive a lock allowed, the server will check whether it is himself who
     * receive the register message initially, if it is, then increment the number
     * of lock-allowed received. If the number of lock-allowd message received
     * matches the number of servers in the whole network, it will send a register
     * success to the client
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveLockAllowed(String msg, Connection con) {
        log.debug("received a LOCK_ALLOWED from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject lockAllo = getJSON(con, msg);
        if (lockAllo == null) {
            return true;
        }
        if (!hasValidKV("username", lockAllo, con)
                || !hasValidKV("secret", lockAllo, con)) {
            return true;
        }
        broadcastWithinServers(con, msg, true);
        String username = (String) lockAllo.get("username");
        String secret = (String) lockAllo.get("secret");
        Map<String, Connection> registeringClients = Control.getInstance()
                .getRegisteringClients();
        if (registeringClients.containsKey(username)) {
            Connection registeringCon = registeringClients.get(username);
            registeringCon.incrementNRequestAllo();
            int nConnectedServers = Control.getInstance()
                    .getInterconnectedServers().size();
            if (registeringCon.getNRequestAllo() == nConnectedServers) {
                String response = sendRegisterSucc(username);
                registeringCon.writeMsg(response);
                Control.getInstance().removeRegisteringClient(username);
                Control.getInstance().addRegisteredClient(username, secret);
            }
        }
        return false;
    }

    /**
     * Newly added message. Generate backup server message to another server
     */
    @SuppressWarnings("unchecked")
    public static String sendBackupServer() {
        JSONObject backupServer = new JSONObject();
        backupServer.put("command", "BACKUP_SERVER");
        backupServer.put("backupname",
                Control.getInstance().getBackupSvNameToSend());
        backupServer.put("backupport", Integer
                .toString(Control.getInstance().getBackupSvPortToSend()));
        return backupServer.toJSONString();
    }

    /**
     * New method.
     */
    public static boolean receiveBackupServer(Connection con, String sMsg) {
        log.debug("received a BACKUP_SERVER from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject backUpSv = getJSON(con, sMsg);
        if (!hasValidKV("backupname", backUpSv, con)
                || !hasValidKV("backupport", backUpSv, con)) {
            return true;
        }
        String backupSvName = (String) backUpSv.get("backupname");
        int backupSvPort = Integer
                .parseInt((String) backUpSv.get("backupport"));
        if (Settings.getLocalHostname().equals(backupSvName)
                && Settings.getLocalPort() == backupSvPort) {
            Control.getInstance().setBackupSvNameToUse(null);
            Control.getInstance().setBackupSvPortToUse(0);
            return false;
        }
        Control.getInstance().setBackupSvNameToUse(backupSvName);
        Control.getInstance().setBackupSvPortToUse(backupSvPort);
        return false;
    }

    /**
     * Newly added message. Reply authentication success to another server
     */
    @SuppressWarnings("unchecked")
    public static String sendAuthenticationSucc() {
        // int port = Settings.getLocalPort();
        JSONObject authenSucc = new JSONObject();
        authenSucc.put("command", "AUTHENTICATION_SUCCESS");
        authenSucc.put("backupname",
                Control.getInstance().getBackupSvNameToSend());
        authenSucc.put("backupport", Integer
                .toString(Control.getInstance().getBackupSvPortToSend()));
        return authenSucc.toJSONString();
    }

    /**
     * New method.
     */
    public static boolean receiveAuthenticationSucc(Connection con,
            String sMsg) {
        log.debug("received an AUTHENTICATION_SUCCESS from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject authenSucc = getJSON(con, sMsg);
        if (!hasValidKV("backupname", authenSucc, con)
                || !hasValidKV("backupport", authenSucc, con)) {
            return true;
        }
        setAuthenSecret(Settings.getSecret());

        con.setConnectingSvName(Settings.getRemoteHostname());
        con.setConnectingSvPort(Settings.getRemotePort());
        Control.getInstance().setBackupSvNameToSend(con.getConnectingSvName());
        Control.getInstance().setBackupSvPortToSend(con.getConnectingSvPort());
        broadcastWithinServers(con, sendBackupServer(), true);

        String backupSvName = (String) authenSucc.get("backupname");
        int backupSvPort = Integer
                .parseInt((String) authenSucc.get("backupport"));
        if (Settings.getLocalHostname().equals(backupSvName)
                && Settings.getLocalPort() == backupSvPort) {
            Control.getInstance().setBackupSvNameToUse(null);
            Control.getInstance().setBackupSvPortToUse(0);
            return false;
        }
        Control.getInstance().setBackupSvNameToUse(backupSvName);
        Control.getInstance().setBackupSvPortToUse(backupSvPort);
        return false;
    }

    /**
     * Version 2: When receive an activity message, process it (add a new field in
     * the activity JSON) and then broadcast it within the whole network.
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveActivityMessage(Connection con, String msg) {
        log.debug("received an ACTIVITY_MESSAGE from "
                + Settings.socketAddress(con.getSocket()));
        JSONObject actMsg = getJSON(con, msg);
        if (actMsg == null) {
            return true;
        }
        if (!hasValidKV("username", actMsg, con)) {
            return true;
        }
        String username = (String) actMsg.get("username");
        if (!username.equals("anonymous")) {
            if (!hasValidKV("secret", actMsg, con)) {
                return true;
            }
        }
        if (notContainsField("activity", actMsg, con)) {
            return true;
        }
        String response;
        JSONObject activity = (JSONObject) actMsg.get("activity");
        if (activity == null) {
            response = sendInvalidMessage(
                    "the received message did not contain the activity value");
            con.writeMsg(response);
            return true;
        }
        if (!validClient(username, actMsg, con)) {
            return true;
        }
        JSONObject processedAct = processActivity(activity, username);

        // store in buff
        String clientAddr = Settings.socketAddress(con.getSocket()); // use client address to identify user
        if (!Control.getInstance().getMsgBuffMap().containsKey(clientAddr)) {
            // not found in map, generate a new object
            Control.getInstance().addMsgBuff(clientAddr, 0);
        }
        int order = Control.getInstance().getMsgBuffMap().get(clientAddr)
                .getNextInMsgOrder(); // generated order
        JSONObject actBroadcast = sendActivityBroadcast(processedAct,
                clientAddr, order);
        long time = (long) actBroadcast.get("time");

        // ****
        Control.getInstance().addBroadcastMsg(time, actBroadcast);
        // ****

        if (!Control.getInstance().getMsgBuffMap().get(clientAddr)
                .put(actBroadcast)) {
            log.warn("wrong msg with previous order received");
            con.writeMsg(
                    sendInvalidMessage("wrong message with previous order"));
            return true;
        }
        // flush the message to broadcast
        while (Control.getInstance().getMsgBuffMap().get(clientAddr).hasNext())
            broadcastToAll(con, Control.getInstance().getMsgBuffMap()
                    .get(clientAddr).flush(), false, time);
        // broadcast to all server

        return false;
    }

    /**
     * Version 2
     * 
     * @param processedAct
     * @return
     */
    @SuppressWarnings("unchecked")
    private static JSONObject sendActivityBroadcast(JSONObject processedAct,
            String clientAddr, int order) {
        long msgTime = (new Date()).getTime();
        JSONObject actBroadcast = new JSONObject();
        actBroadcast.put("command", "ACTIVITY_BROADCAST");
        actBroadcast.put("activity", processedAct);
        actBroadcast.put("time", msgTime);
        actBroadcast.put("client", clientAddr);
        actBroadcast.put("order", order);
        return actBroadcast;
    }

    /**
     * Version 2: When receive an activity broadcast, forward it.
     * 
     * @return whether this connection should terminate or not
     */
    public static boolean receiveActivityBroadcast(Connection con,
            String msg) {
        log.debug("received an ACTIVITY_BROADCAST from "
                + Settings.socketAddress(con.getSocket()));
        if (!validServer(con)) {
            return true;
        }
        JSONObject actBroadCast = getJSON(con, msg);
        if (actBroadCast == null) {
            return true;
        }
        if (notContainsField("activity", actBroadCast, con)) {
            return true;
        }
        if (notContainsField("time", actBroadCast, con)) {
            return true;
        }
        long time = (long) actBroadCast.get("time");
        JSONObject activity = (JSONObject) actBroadCast.get("activity");
        if (activity == null) {
            String response = sendInvalidMessage(
                    "the received message did not contain the activity value");
            con.writeMsg(response);
            return true;
        }
        String clientAddr = (String) actBroadCast.get("client");
        int order = ((Number) actBroadCast.get("order")).intValue();
        if (!Control.getInstance().getMsgBuffMap().containsKey(clientAddr)) // not found in map, generate a new object
            Control.getInstance().addMsgBuff(clientAddr, order);
        if (!Control.getInstance().getMsgBuffMap().get(clientAddr)
                .put(actBroadCast)) { // put failed, wrong order
            log.warn("wrong msg with previous order received");
            con.writeMsg(
                    sendInvalidMessage("wrong message with previous order"));
            return true;
        }
        // flush the message to broadcast
        while (Control.getInstance().getMsgBuffMap().get(clientAddr)
                .hasNext()) {
            // broadcast to all except the con sent msg
            broadcastToAll(con, Control.getInstance().getMsgBuffMap()
                    .get(clientAddr).flush(), true, time);
        }
        Control.getInstance().addBroadcastMsg(time, actBroadCast);
        return false;
    }

    /**
     * process the activity message
     * 
     * @param activity
     *            which to be processed
     * @param username
     *            which to be added into activity message
     * @return processed activity message
     */
    @SuppressWarnings("unchecked")
    private static JSONObject processActivity(JSONObject activity,
            String username) {
        activity.put("authenticated_user", username);
        return activity;
    }

    /**
     * 
     * @return whether it is a valid user (has logged in)
     */
    private static boolean validClient(String username, JSONObject actMsg,
            Connection con) {
        String clientSockAddr = Settings.socketAddress(con.getSocket());
        String response;
        if (username.equals("anonymous")) {
            if (!Control.getInstance().getLoggedinAnonymous()
                    .contains(clientSockAddr)) {
                response = sendAuthenticationFail(
                        "sending ACTIVITY_MESSAGE without logging in first");
                con.writeMsg(response);
                return false;
            }
        } else {
            String secret = (String) actMsg.get("secret");
            String identifier = username + ":" + secret;
            Map<String, String> loggedinClients = Control.getInstance()
                    .getLoggedinClients();
            if (!loggedinClients.containsKey(identifier) || !loggedinClients
                    .get(identifier).equals(clientSockAddr)) {
                response = sendAuthenticationFail(
                        "sending ACTIVITY_MESSAGE without logging in first"
                                + " or the supplied secret is incorrect: "
                                + secret);
                con.writeMsg(response);
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * @return whether it is a valid server (has been authenticated)
     */
    private static boolean validServer(Connection con) {
        String receiveFrom = Settings.socketAddress(con.getSocket());
        if (!Control.getInstance().getAuthenticatedServers()
                .contains(receiveFrom)) {
            String response = sendInvalidMessage(
                    "need to be authenticated first");
            con.writeMsg(response);
            return false;
        }
        return true;
    }

    public static boolean hasSecret() {
        if (Settings.getSecret() == null) {
            return false;
        }
        log.info("using given secret: " + Settings.getSecret());
        return true;
    }

    /**
     * Version 2: Broadcast to all nodes (including clients) within network.
     * 
     * @param forwardMsg
     *            whether needs to send to the sender
     */
    private static void broadcastToAll(Connection con, String msg,
            boolean forwardMsg, long time) {
        ArrayList<Connection> connections = Control.getInstance()
                .getConnections();
        String receivedFrom = Settings.socketAddress(con.getSocket());
        if (forwardMsg) {
            for (Connection c : connections) {
                if (c.getEstablishTime() <= time) {
                    String socAddr = Settings.socketAddress(c.getSocket());
                    if (!socAddr.equals(receivedFrom))
                        c.writeMsg(msg);
                }
            }
        } else {
            for (Connection c : connections) {
                if (c.getEstablishTime() <= time)
                    c.writeMsg(msg);
            }
        }
    }

    /**
     * Version 2: Broadcast just within servers.
     * 
     * @param forwardMsg
     *            whether needs to send to the sender
     */
    public static void broadcastWithinServers(Connection con, String msg,
            boolean forwardMsg) {
        ArrayList<Connection> connections = Control.getInstance()
                .getConnections();
        String receivedFrom = Settings.socketAddress(con.getSocket());
        if (forwardMsg) {
            for (Connection c : connections) {
                String socAddr = Settings.socketAddress(c.getSocket());
                if (c.isServer() && (!socAddr.equals(receivedFrom)))
                    c.writeMsg(msg);
            }
        } else {
            for (Connection c : connections) {
                if (c.isServer())
                    c.writeMsg(msg);
            }
        }
    }

    /**
     * Check whether a JSONObject contains a certain field and has values.
     * 
     * @param field
     *            the field we want to check whether a JSONObject has
     * @param jMsg
     *            the JSONObject to be checked
     * @param con
     * @return whether the JSONObject contains this certain field and has values or
     *         not
     */
    private static boolean hasValidKV(String field, JSONObject jMsg,
            Connection con) {
        if (notContainsField(field, jMsg, con)) {
            return false;
        }
        String value = (String) jMsg.get(field);
        if (notContainsValue(value, field, con)) {
            return false;
        }
        return true;
    }

    private static boolean notContainsField(String field, JSONObject jMsg,
            Connection con) {
        if (!jMsg.containsKey(field)) {
            String response = sendInvalidMessage(
                    "the received message did not contain the " + field
                            + " field");
            con.writeMsg(response);
            return true;
        }
        return false;
    }

    private static boolean notContainsValue(String value, String field,
            Connection con) {
        if (value == null) {
            String response = sendInvalidMessage(
                    "the received message did not contain the " + field
                            + " value");
            con.writeMsg(response);
            return true;
        }
        return false;
    }

    /**
     * calculate the local load (how many clients connecting to it)
     */
    private static int localLoad(ArrayList<Connection> connections) {
        int load = 0;
        for (Connection connect : connections) {
            if (!connect.isServer()) {
                load++;
            }
        }
        return load;
    }

    public static JSONObject getJSON(Connection con, String sMsg) {
        try {
            JSONObject jMsg = (JSONObject) parser.parse(sMsg);
            return jMsg;
        } catch (ParseException p) {
            String response = sendInvalidMessage(
                    "JSON parse error while parsing message");
            con.writeMsg(response);
            return null;
        }
    }

    public static String getCommandName(Connection con, String sMsg) {
        String sCmd = "";
        JSONObject jMsg = getJSON(con, sMsg);
        if (jMsg == null) {
            return sCmd;
        }
        if (jMsg.containsKey("command")) {
            sCmd = (String) jMsg.get("command");
        } else {
            String response = sendInvalidMessage(
                    "the received message did not contain the command field");
            con.writeMsg(response);
        }
        return sCmd;
    }

}
