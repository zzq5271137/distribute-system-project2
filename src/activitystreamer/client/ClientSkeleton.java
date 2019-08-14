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

package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.rmi.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
    private static final Logger log = LogManager.getLogger();
    private static ClientSkeleton clientSolution;
    private TextFrame textFrame;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private BufferedReader inreader;
    private PrintWriter outwriter;
    private JSONParser parser = new JSONParser();
    private boolean term = false;
    private boolean loginFlag = false;

    public static ClientSkeleton getInstance() {
        if (clientSolution == null) {
            clientSolution = new ClientSkeleton();
        }

        return clientSolution;
    }

    public ClientSkeleton() {
        try {
            socket = new Socket(Settings.getRemoteHostname(),
                    Settings.getRemotePort());
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
            inreader = new BufferedReader(new InputStreamReader(in));
            outwriter = new PrintWriter(out, true);
            start();
            initialAuthenticate();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            disconnect();
        } catch (IOException e) {
            e.printStackTrace();
            disconnect();
        }
    }

    private void writeMsg(JSONObject Obj) {
        if (!socket.isClosed()) {
            outwriter.println(Obj.toJSONString());
            outwriter.flush();
            log.info(this.socket.getLocalSocketAddress() + " sent: --- " + Obj
                    + "--- to " + this.socket.getRemoteSocketAddress());
        }
    }

    @SuppressWarnings("unchecked")
    public void sendActivityObject(JSONObject activityObj) {
        JSONObject fullObj = new JSONObject();
        fullObj.put("command", "ACTIVITY_MESSAGE");
        fullObj.put("username", Settings.getUsername());
        if (Settings.getUsername() != "anonymous") {
            fullObj.put("secret", Settings.getSecret());
            fullObj.put("activity", activityObj);
            writeMsg(fullObj);
        } else {
            fullObj.put("secret", Settings.getSecret());
            fullObj.put("activity", activityObj);
            writeMsg(fullObj);
        }
    }

    public void disconnect() {
        if (socket != null) {
            try {
                logout();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void run() {
        try {
            String data;
            while (!term && (data = inreader.readLine()) != null) {
                JSONObject obj = (JSONObject) parser.parse(data);
                process(obj);
                if (loginFlag && obj.get("command").toString()
                        .equals("ACTIVITY_BROADCAST")) {
                    JSONObject actObj = (JSONObject) parser
                            .parse(obj.get("activity").toString());
                    textFrame.setOutputText(actObj);
                }
            }
            in.close();
        } catch (IOException e) {
            log.error("connection " + Settings.socketAddress(socket)
                    + " closed with exception: " + e);
        } catch (ParseException e1) {
            log.error(
                    "invalid JSON object entered into input text field, data not sent");

            JSONObject inMsg = new JSONObject();
            inMsg.put("command", "INVALID_MESSAGE");
            inMsg.put("info", "JSON parse error while parsing message");
            writeMsg(inMsg);

            disconnect();
        }
    }

    @SuppressWarnings("unchecked")
    public void process(JSONObject obj) {
        try {
            String Comm = obj.get("command").toString();
            switch (Comm) {
            case "REGISTER_SUCCESS":
                if (obj.containsKey("info") && !obj.containsValue(null)) {
                    log.debug(obj.toString());
                    JSONObject login = new JSONObject();
                    loginObj(login);
                    log.debug(login.toString());
                    writeMsg(login);
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "REGISTER_SUCCESS incorrect");
                    writeMsg(inMsg);
                }
                break;
            case "REDIRECT":
                if (obj.containsKey("hostname") && obj.containsKey("port")
                        && !obj.containsValue(null)) {
                    if (loginFlag) {
                        disconnect();
                        String newRemoteName = (String) obj.get("hostname");
                        int newRemotePort = Integer
                                .parseInt(obj.get("port").toString());
                        Settings.setRemoteHostname(newRemoteName);
                        Settings.setRemotePort(newRemotePort);
                        socket = new Socket(newRemoteName, newRemotePort);
                        in = new DataInputStream(socket.getInputStream());
                        out = new DataOutputStream(socket.getOutputStream());
                        inreader = new BufferedReader(
                                new InputStreamReader(in));
                        outwriter = new PrintWriter(out, true);
                        textFrame.dispose();
                        initialAuthenticate();
                        log.debug("Redirecting to: " + obj.toString());
                    }
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "REDIRECT is incorrect");
                    writeMsg(inMsg);
                }
                break;
            case "LOGIN_SUCCESS":
                if (obj.containsKey("info") && !obj.containsValue(null)) {
                    log.debug(obj.toString());
                    loginFlag = true;
                    textFrame = new TextFrame();
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "LOGIN_SUCCESS is incorrect");
                    writeMsg(inMsg);
                }
                break;
            case "AUTHENTICATION_FAIL":
                if (obj.containsKey("info") && !obj.containsValue(null)) {
                    log.debug(obj.toString());
                    disconnect();
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "AUTHENTICATION_FAIL is incorrect");
                    writeMsg(inMsg);
                }
                break;
            case "LOGIN_FAILED":
                if (obj.containsKey("info") && !obj.containsValue(null)) {
                    log.debug(obj.toString());
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "LOGIN_FAILED is incorrect");
                    writeMsg(inMsg);
                }
                break;
            case "REGISTER_FAILED":
                if (obj.containsKey("info") && !obj.containsValue(null)) {
                    log.debug(obj.toString());
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "REGISTER_FAILED is incorrect");
                    writeMsg(inMsg);
                }
                break;
            case "ACTIVITY_BROADCAST":
                if (obj.containsKey("activity") && !obj.containsValue(null)) {
                    log.debug("Activity from broadcast: " + obj.toString());
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "ACTIVITY_BROADCAST is incorrect");
                    writeMsg(inMsg);
                }
                break;
            case "INVALID_MESSAGE":
                if (obj.containsKey("info") && !obj.containsValue(null)) {
                    log.debug(obj.toString());
                    disconnect();
                } else {
                    JSONObject inMsg = new JSONObject();
                    inMsg.put("command", "INVALID_MESSAGE");
                    inMsg.put("info", "INVALID_MESSAGE is incorrect");
                    writeMsg(inMsg);
                }
                break;
            default:
                JSONObject inMsg = new JSONObject();
                inMsg.put("command", "INVALID_MESSAGE");
                inMsg.put("info",
                        "the received message did not contain a right command");
                writeMsg(inMsg);
                log.debug("Command of message is incorrect!!!");
                disconnect();
                break;
            }
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
            disconnect();
        } catch (IOException e2) {
            e2.printStackTrace();
            disconnect();
        }
    }

    @SuppressWarnings("unchecked")
    private void initialAuthenticate() {
        JSONObject authen = new JSONObject();
        if (Settings.getUsername() != null && Settings.getSecret() != null) {
            loginObj(authen);
            log.debug(authen.toString());
        } else if (Settings.getSecret() == null) {
            if (Settings.getUsername() != "anonymous") {
                String secret = Settings.nextSecret();
                authen.put("command", "REGISTER");
                authen.put("username", Settings.getUsername());
                authen.put("secret", secret);
                Settings.setSecret(secret);
                log.debug("Your secret is: " + secret);
            } else {
                authen.put("command", "LOGIN");
                authen.put("username", "anonymous");
            }
        }
        writeMsg(authen);
    }

    @SuppressWarnings("unchecked")
    private void loginObj(JSONObject login) {
        login.put("command", "LOGIN");
        login.put("username", Settings.getUsername());
        login.put("secret", Settings.getSecret());
    }

    @SuppressWarnings("unchecked")
    private void logout() {
        JSONObject logout = new JSONObject();
        logout.put("command", "LOGOUT");
        writeMsg(logout);
        log.debug("Logout on " + Settings.getRemoteHostname() + ": "
                + Settings.getRemotePort());
    }

}
