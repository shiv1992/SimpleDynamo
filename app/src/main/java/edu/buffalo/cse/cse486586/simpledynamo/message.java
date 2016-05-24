package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by shivang on 4/11/16.
 */
public class message {
    public String key;
    public String mess;
    public String type;
    public String port;
    public String cnt;
    public HashMap<String,String> hm;


    public String toJString() throws JSONException {

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("KEY",this.key);
        jsonObj.put("VALUE", this.mess);
        jsonObj.put("TYPE", this.type);
        jsonObj.put("PORT", this.port);
        jsonObj.put("CNT", this.cnt);

        hm=new HashMap<String, String>();

        JSONObject innerJsonObj = new JSONObject();
        for(String strKey:hm.keySet()){
            innerJsonObj.put(strKey,hm.get(strKey));
        }

        jsonObj.put("CUR",innerJsonObj);
        return jsonObj.toString();
    }

    //Cursor String
    public String toJString(Cursor cur,HashMap<String,String> nm) throws JSONException {


        JSONObject jsonObj = new JSONObject();
        jsonObj.put("PORT", this.port);
        jsonObj.put("TYPE", this.type);
        jsonObj.put("KEY", this.key);
        jsonObj.put("VALUE", this.mess);
        jsonObj.put("CNT", this.cnt);

        hm=new HashMap<String, String>();
        //cur.moveToFirst();
        if(nm==null) {
           // Log.v("CUR CUNVERT", "Enter");
           // Log.v("CUR CUNVERT SIZE", String.valueOf(cur.getCount()));

            while (cur.moveToNext()) {
             //   Log.v("CUR CUNVERT","1");
                hm.put(cur.getString(cur.getColumnIndex("key")), cur.getString(cur.getColumnIndex("value")));
            }
            JSONObject innerJsonObj = new JSONObject();
            for(String strKey:hm.keySet()){
               // Log.v("CUR CUNVERT","2");
                innerJsonObj.put(strKey,hm.get(strKey));
               // Log.v("KEY", strKey);
               // Log.v("VAL",hm.get(strKey));
            }
            jsonObj.put("CUR",innerJsonObj);

        }
        else
        {
          //  Log.v("CUR CUNVERT SIZE", String.valueOf(cur.getCount()));
           // Log.v("CUR CUNVERT SSIZE", String.valueOf(nm.size()));

            while (cur.moveToNext()) {
             //   Log.v("CUR CUNVERT","1N");
                nm.put(cur.getString(cur.getColumnIndex("key")), cur.getString(cur.getColumnIndex("value")));
            }
            JSONObject innerJsonObj = new JSONObject();

            for(String strKey:nm.keySet()){
               // Log.v("CUR CUNVERT","2N");
                innerJsonObj.put(strKey, nm.get(strKey));
               // Log.v("KEY", strKey);
               // Log.v("VAL", nm.get(strKey));
            }
            jsonObj.put("CUR",innerJsonObj);


        }



        return jsonObj.toString();
    }

    public void toJMsg(String in) throws JSONException {


        if (in!=null) {
            JSONObject obj = new JSONObject(in);
            String key = null;
            if (obj.getString("KEY")!=null) {
                key = obj.getString("KEY");
                Log.v("OUT KEY",key);
            }
            String value = null;
            if (obj.getString("VALUE")!=null) {
                value = obj.getString("VALUE");
            }

            String type = null;
            if (obj.getString("TYPE")!=null) {
                type = obj.getString("TYPE");
            }
            String port = null;
            if (obj.getString("PORT")!=null) {
                port = obj.getString("PORT");
            }
            String cnt = null;
            if (obj.getString("CNT")!=null) {
                cnt = obj.getString("CNT");
            }

            int ab = obj.getJSONObject("CUR").length();
            Log.v("Out CNT AB: ", String.valueOf(ab));
            JSONObject jObject = obj.getJSONObject("CUR");
            Log.v("OUT", jObject.toString());


            if (jObject.toString()!=null) {
                Iterator<String> keys = jObject.keys();
                hm = new HashMap<String, String>();

                while (keys.hasNext()) {
                    String K = keys.next();
                    Log.v("OUT K", K);
                    String V = jObject.getString(K);
                    Log.v("OUT V", V);
                    hm.put(K, V);
                }

            }
            Log.v("OUT AFTER", hm.toString());


            this.key = key;
            this.mess = value;
            this.type = type;
            this.port = port;
            this.cnt = cnt;


        }
    }
}
