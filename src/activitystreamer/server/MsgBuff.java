/**
 * This class buffers messages for a single client.
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

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

public class MsgBuff {
    private int nextInMsgOrder;
    private int nextOutMsgOrder;
    private Map<Integer, JSONObject> jMsgList;

    MsgBuff(int order) {
        this.jMsgList = new HashMap<Integer, JSONObject>();
        this.nextInMsgOrder = order;
        this.nextOutMsgOrder = order;
    }

    public int getNextInMsgOrder() {
        return nextInMsgOrder;
    }

    public boolean put(JSONObject jMsg) {
        if (((Number) jMsg.get("order")).intValue() == nextInMsgOrder) { // msg in order
            jMsgList.put(new Integer(nextInMsgOrder), jMsg);
            while (true) { // move nextInMsgOrder to next msg order gap
                if (!jMsgList.containsKey(new Integer(nextInMsgOrder)))
                    break;
                nextInMsgOrder++;
            }
            return true;
        } else if (((Number) jMsg.get("order")).intValue() > nextInMsgOrder) { // msg not in order
            Map<Integer, JSONObject> jMsgWithOrder = new HashMap<Integer, JSONObject>();
            jMsgWithOrder.put(new Integer(nextInMsgOrder), jMsg);
            return true;
        } else { // order < next order, previous msg received or wrong order, discard
            return false;
        }
    }

    public String flush() {
        if (nextOutMsgOrder < nextInMsgOrder) { // unsent in order msg exist
            JSONObject jMsg = jMsgList.get(new Integer(nextOutMsgOrder));
            jMsgList.remove(new Integer(nextOutMsgOrder));
            nextOutMsgOrder++;
            return jMsg.toString();
        } else { // no message needs to be sent or wrong order
            return null;
        }
    }

    public boolean hasNext() {
        return (nextOutMsgOrder < nextInMsgOrder) ? true : false;
    }

}
