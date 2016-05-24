package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Time;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;

public class SimpleDynamoProvider extends ContentProvider {

	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";
	public static SQLiteDatabase db=null;
	public static DbHelper dbS;
	public static String TableName = "myTable";
	static final int SERVER_PORT = 10000;
	public static Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider/myTable");
    public static String[] Node={"5554","5556","5558","5560","5562"};
    public static String[] NodeH={"","","","",""};
    public static String[] Port={"11108","11112","11116","11120","11124"};
    public static boolean[] Stat={true,true,true,true,true};
    public static String Status="11111";
    public static int POS=1;
    private static String myPort="";
    public static String pred;
    public static String succ;
    public static int INIT=0;
    public static message mesg=null,mesg2=null,mesg3=null;//,TTT;
    private final String TTT="Hi",TTT1="Hello",LOCK="LOCK";
    private String lock="false",act="testing";
    private int count1=0,count2=0;
    private int queryLock=0;



    public static int STARFLAG=0;//,STARFLAG=0; // Flag to set if the avd was the initiator for *


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();

        Log.e("1","Before");
        db = dbS.getReadableDatabase();
        Log.e("3", "Middle");

        queryBuilder.setTables(TableName);
        Log.e("4", "Again middle");

        Cursor cr;
        db.execSQL("DELETE FROM "+TableName);
        cr=db.rawQuery("SELECT * FROM "+TableName,null);
        Log.v("DELETE COUNT",String.valueOf(cr.getCount()));
        return cr.getCount();
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

            //Query Lock
            queryLock=1;

            // Get key Value
            String key = (String) values.get("key");
            String value = (String) values.get("value");

            String FinPort = "";
            int i, j;

            String NodeHash = "";
            //Generate Hash String
            try {
                NodeHash = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        /*
            Check for correct partition
        */

            //Check for first partition
            if (NodeHash.compareTo(NodeH[0]) < 0 || NodeHash.compareTo(NodeH[4]) > 0) {
                // Send to first
                FinPort = Port[0];

            } else {
                int flag = 0;
                for (i = 0; i <= 4; i++) {

                    for (j = i + 1; j <= 4; j++) {

                        if (NodeHash.compareTo(NodeH[i]) > 0 && NodeHash.compareTo(NodeH[j]) < 0) {
                            flag = 1;
                            FinPort = Port[j];
                        }


                        if (flag == 1) {
                            break;
                        }
                    }


                    if (flag == 1) {
                        break;
                    }

                }
            }

            //Send Message to FinPort
            message tmp = new message();
            tmp.type = "2";
            tmp.key = key;
            tmp.port = FinPort;
            tmp.mess = value;
            tmp.cnt = "2";
            String send = "";
            try {
                send = tmp.toJString();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            Log.v("Sent to FinPort for " + value + " : ", FinPort);
            new ClientTaskDirect().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, FinPort);




        return null;
	}

    /// Insert Current
    public Uri insertD(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        System.currentTimeMillis();

        // Get key Value
        db = dbS.getWritableDatabase();
        Log.v("DELINSERT K",values.getAsString("key"));
        Log.v("DELINSERT V", values.getAsString("value"));
        long sat=db.insertWithOnConflict(TableName, null, values, CONFLICT_REPLACE);
        Log.v("DELINSERT SAT",String.valueOf(sat));
        return null;
    }



	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        Log.e("Ins", "Ins");
        dbS = new DbHelper(this.getContext());

        //delete(uri,null,null);

        //Telephony
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        //Setting myPort as Pred and Succ at Start
        pred=myPort;
        succ=myPort;

        //Set Time



        // SORT Acc Hash Valve
        int i,j;
        for(i=0;i<5;i++)
        {
            try {
                NodeH[i]=genHash(Node[i]);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        String str="";
        for(i=0;i<5;i++)
        {
            for(j=i+1;j<5;j++)
            {
                if(NodeH[i].compareTo(NodeH[j]) >0)
                {
                    str=NodeH[i];
                    NodeH[i]=NodeH[j];
                    NodeH[j]=str;

                    str=Node[i];
                    Node[i]=Node[j];
                    Node[j]=str;

                    str=Port[i];
                    Port[i]=Port[j];
                    Port[j]=str;
                }

            }
        }

        for(i=0;i<5;i++){
            if(myPort.equals(Port[i]))
            {
                POS=i;
            }
            Log.v("TT NODE "+i," :"+Node[i]);
            Log.v("TT NODEH "+i," :"+NodeH[i]);
            Log.v("TT PORT "+i," :"+Port[i]);


        }

        if(POS==0)
        {
            pred=Port[4];
            succ=Port[1];
        }
        else if(POS==4)
        {
            pred=Port[3];
            succ=Port[0];
        }
        else
        {
            pred=Port[POS-1];
            succ=Port[POS+1];
        }

        Log.v("PORT PRED : ", pred);
        Log.v("PORT SUCC : ",succ);

        //Send Activation to port 5554 Client Task
        //new ClientTaskActive0().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "ACTIVE", myPort);


        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        /*
        * Re Generation of Dead Node
        * */

        //Get Positions
        int a,b;

        a=POS+1;
        if(a>4)
            a=0;
        b=a+1;
        if(b>4)
            b=0;

        Log.v("PR PR",String.valueOf(POS));
        Log.v("PR A",String.valueOf(a));
        Log.v("PR B",String.valueOf(b));

           //synchronized(lock)
           {
               lock="true";

            /*
            * Query Predesscor "PR"
            * */
            Cursor PR=query(uri, null, "PR", null, null);
            Log.v("PR Count",String.valueOf(PR.getCount()));

            int count=0;
            //Get values to insert into DB
            if(PR.getCount()>0) {
                PR.moveToFirst();
                while(true)
                {
                    String key=PR.getString(PR.getColumnIndex("key"));
                    String val=PR.getString(PR.getColumnIndex("value"));
                    String genH="";

                    try {
                        genH=genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    if(a==0 || b==0)
                    {
                        if(genH.compareTo(NodeH[POS]) <=0 && genH.compareTo(NodeH[b]) >0)
                        {
                            Log.v("AB = ","0");
                            ContentValues CV=new ContentValues();
                            CV.put("key",key);
                            CV.put("value",val);
                            insertD(uri,CV);
                            count++;
                        }
                    }
                    else
                    {
                        if(genH.compareTo(NodeH[POS]) <=0 || genH.compareTo(NodeH[b]) >0)
                        {
                            Log.v("POS = ","0");
                            ContentValues CV=new ContentValues();
                            CV.put("key",key);
                            CV.put("value",val);
                            insertD(uri, CV);
                            count++;
                        }
                    }



                    if(PR.isLast())
                        break;
                    else
                        PR.moveToNext();
                }

                Log.v("PX Count", String.valueOf(count));
            }


            //Query Successor "NX"
            Cursor NX=query(uri, null, "NX", null, null);
            Log.v("NX Count", String.valueOf(NX.getCount()));
            //Get values to insert into DB
            count=0;
            if(NX.getCount()>0) {
                NX.moveToFirst();
                while (true) {
                    String key = NX.getString(PR.getColumnIndex("key"));
                    String val = NX.getString(PR.getColumnIndex("value"));
                    String genH = "";

                    try {
                        genH = genHash(key);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    if (a == 0 || b == 0) {
                        if (genH.compareTo(NodeH[POS]) <= 0 && genH.compareTo(NodeH[b]) > 0) {
                            Log.v("AB = ", "0");
                            ContentValues CV = new ContentValues();
                            CV.put("key", key);
                            CV.put("value", val);
                            insertD(uri, CV);
                            count++;
                        }
                    } else {
                        if (genH.compareTo(NodeH[POS]) <= 0 || genH.compareTo(NodeH[b]) > 0) {
                            Log.v("POS = ", "0");
                            ContentValues CV = new ContentValues();
                            CV.put("key", key);
                            CV.put("value", val);
                            insertD(uri, CV);
                            count++;
                        }
                    }


                    if (NX.isLast())
                        break;
                    else
                        NX.moveToNext();
                }

                Log.v("NX Count", String.valueOf(count));

            }
           lock="false";
           synchronized (act){
               act.notify();
           }

        }//Lock end

        } catch (IOException e) {

            Log.e("TAG", "Can't create a ServerSocket");
            //return false;
        }

        return true;
	}



    /*
    * Query
    *
    * */

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();

        Log.e("1","Before");
        db = dbS.getReadableDatabase();
        Log.e("3", "Middle");

        queryBuilder.setTables(TableName);

        Log.e("4", "Again middle");
        Log.e("SEL KEY", selection);

        Cursor cursor;
        MatrixCursor mat = null;
        MergeCursor mgr=null;



        if(selection.equals("PR") )
        {
            Log.e("PR", "Enter");
            //cursor=db.rawQuery("SELECT * FROM " + TableName,null);

            //Returning DB of all Databases
            message curmess=new message();
            curmess.port=myPort;
            curmess.type="7";
            curmess.mess=pred;
            curmess.key="";
            curmess.cnt="0";
            String send="";
            try {
                send=curmess.toJString();
                Log.v("START","PR");
            } catch (JSONException e) {
                e.printStackTrace();
            }


            //Wait for Object
            //TTT=new message();
            synchronized(TTT)
            {
                try {
                    Log.v("NEXT", "PORT");
                    new ClientTaskSendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, "7");


                    TTT.wait();

                    //Matrix Cursor
                    String[] tmp = {"key", "value"};
                    mat = new MatrixCursor(tmp);
                    Log.v("NEXT", "PORT START");
                    try {
                        Log.v("NEXT", "PORT START MSG " + mesg.hm.toString());
                    }catch(NullPointerException e)
                    {
                        Log.e("NULL : ", "EXCEPTION");
                    }

                    if (mesg.hm!=null && !mesg.hm.isEmpty())
                    {
                        Log.v("NEXT", "PORT START Start");
                        Iterator it = mesg.hm.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry) it.next();
                            //System.out.println(pair.getKey() + " = " + pair.getValue());
                            String[] str = {pair.getKey().toString(), pair.getValue().toString()};
                            Log.v("PR KEY",str[0]);
                            Log.v("PR VAL",str[1]);
                            mat.addRow(str);
                        }
                    }
                    }catch(InterruptedException e){
                    e.printStackTrace();
                }
                }



            Cursor[] out={mat};
            mgr=new MergeCursor(out);

            //Return MatrixCursor as Cursor
            return mgr;

        }

        // For Getting DB of Successor Node
        else if(selection.equals("NX") )
        {
            Log.e("NX", "Enter");
            //cursor=db.rawQuery("SELECT * FROM " + TableName,null);

            //Returning DB of all Databases
            message curmess=new message();
            curmess.port=myPort;
            curmess.type="7";
            curmess.mess=succ;
            curmess.key="";
            curmess.cnt="0";
            String send="";
            try {
                send=curmess.toJString();
                Log.v("START","NX");
            } catch (JSONException e) {
                e.printStackTrace();
            }


            //Wait for Object
            //TTT=new message();
            synchronized(TTT)
            {
                try {
                    Log.v("NEXT", "PORT");
                    new ClientTaskSendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, "7");

                    TTT.wait();

                    //Matrix Cursor
                    String[] tmp={"key","value"};
                    mat=new MatrixCursor(tmp);
                    Log.v("NEXT", "PORT START");
                    try {
                        Log.v("NEXT", "PORT START MSG " + mesg.hm.toString());
                    }catch(NullPointerException e)
                    {
                        Log.e("NULL : ", "EXCEPTION");
                    }


                    if (mesg.hm!=null && !mesg.hm.isEmpty())
                    {
                        Log.v("NEXT", "PORT START Start");
                        Iterator it = mesg.hm.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry) it.next();
                            //System.out.println(pair.getKey() + " = " + pair.getValue());
                            String[] str = {pair.getKey().toString(), pair.getValue().toString()};
                            Log.v("NX KEY",str[0]);
                            Log.v("NX VAL",str[1]);
                            mat.addRow(str);
                        }
                    }
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }


            Cursor[] out={mat};
            mgr=new MergeCursor(out);

            //Return MatrixCursor as Cursor
            return mgr;


        }


        //For @
        else if(selection.equals("@") )
        {
            Log.e("@", "Enter");
            cursor=db.rawQuery("SELECT * FROM " + TableName,null);
            return cursor;
        }
        //For * Initial
        else if(selection.equals("*") )
        {
            STARFLAG=0;
            Log.e("*", "Enter");
            cursor=db.rawQuery("SELECT * FROM " + TableName,null);
            Log.v("Get Count :",String.valueOf(cursor.getCount()));
            //Returning DB of all Databases
            message curmess=new message();
            curmess.port=myPort;
            curmess.type="4";
            curmess.mess="A";
            curmess.key="";
            curmess.cnt="0";
            String send="";
            try {
                send=curmess.toJString(cursor, null);
                Log.v("START","*");
            } catch (JSONException e) {
                e.printStackTrace();
            }


            //Wait for Object
            //TTT=new message();
            synchronized(TTT1)
            {
                try {
                    Log.v("NEXT", "PORT");
                    new ClientTaskReplicate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, myPort);
                    //while(STARFLAG!=1)
                    {
                        TTT1.wait();
                    }
                    //Matrix Cursor
                    String[] tmp={"key","value"};
                    mat=new MatrixCursor(tmp);
                    int cnt=0;
                    Iterator it = mesg.hm.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry pair = (Map.Entry) it.next();
                        //System.out.println(pair.getKey() + " = " + pair.getValue());
                        String[] str={pair.getKey().toString(),pair.getValue().toString()};
                        mat.addRow(str);
                        cnt++;
                    }
                    Log.v("Final Count : ",String.valueOf(cnt));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            Cursor[] out={mat};
            mgr=new MergeCursor(out);

            //Return MatrixCursor as Cursor
            return mgr;
        }
        //For * Forward
        else if (selection.equals("#") )
        {
            Log.e("*", "Enter");
            cursor=db.rawQuery("SELECT * FROM " + TableName,null);
            Log.v("Get Count :",String.valueOf(cursor.getCount()));

            //Returning DB of all Databases
            message curmess=new message();
            curmess.port=mesg.port;
            curmess.type="4";
            curmess.mess="A";
            curmess.key="";
            curmess.cnt="0";
            String send="";
            try {
                send=curmess.toJString(cursor, mesg.hm);
                Log.v("START","*");
            } catch (JSONException e) {
                e.printStackTrace();
            }

            //Send Object
            Log.v("NEXT", "PORT");
            new ClientTaskReplicate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, myPort);
            return null;

        }


        else if(selection.equals("$") ){
            queryBuilder.appendWhere(KEY_FIELD + " = " + "'" + sortOrder + "'");

            Log.e("$", "ENTER");

            Log.e(" Appendwhere", queryBuilder.buildQuery(null, null, null, null, null, null));

            cursor = queryBuilder.query(db, projection, null, null, null, null, null);
            Log.v("$$ : ", String.valueOf(cursor.getCount()));
            return cursor;


        }

        else {

           // STARFLAG=0;
            queryBuilder.appendWhere(KEY_FIELD + " = " + "'" + selection + "'");

            count1++;
            Log.e("SINGLE", "ENTER");
            Log.e(" Appendwhere", queryBuilder.buildQuery(null, null, null, null, null, null));

            cursor = queryBuilder.query(db, projection, null, null, null, null, null);



                String FinPort="";

                int i,j;

                String NodeHash="";
                //Generate Hash String
                try {
                    NodeHash=genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

        /*
            Check for correct partition
        */

                //Check for first partition
                if(NodeHash.compareTo(NodeH[0])<0 || NodeHash.compareTo(NodeH[4])>0)
                {
                    // Send to first
                    FinPort=Port[0];

                }
                else {
                    int flag = 0;
                    for (i = 0; i <= 5; i++) {

                            for (j = i + 1; j <= 4; j++) {

                                    if (NodeHash.compareTo(NodeH[i]) > 0 && NodeHash.compareTo(NodeH[j]) < 0) {
                                        flag = 1;
                                        FinPort = Port[j];
                                    }


                                if (flag == 1) {
                                    break;
                                }
                            }



                        if (flag == 1) {
                            break;
                        }

                    }

                }


                message curmess=new message();
                curmess.port=myPort;
                curmess.key=selection;
                curmess.type="5";
                curmess.mess=FinPort;
                curmess.cnt="0";
                String send="";

                try {
                    send=curmess.toJString();

                    Log.v("START","*");
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                //Wait for Object

                        Log.v("NEXT", "BEFore : " +selection);
                        Log.v("COUNT", String.valueOf(cursor.getCount()));
                        Log.v("NEXT", send);
                        Log.v("COUNT1 BEFORE "+ selection +" : ", String.valueOf(count1));
                        Log.v("COUNT2 BEFORE "+ selection +" : ", String.valueOf(count2));
                    synchronized(TTT)
                    {


                        try {
                            new ClientTaskSendTo().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, send, "2");


                            TTT.wait();

                            count2++;

                            message mesg1=new message();
                            mesg1=mesg;
                            Log.v("COUNT1 AFTER  "+ mesg1.key +" : ", String.valueOf(count1));
                            Log.v("COUNT2 AFTER "+ mesg1.key +" : ", String.valueOf(count2));
                            if(mesg1.hm.isEmpty())
                            {
                                Log.v("NEXT MESSAGE","EMPTY for : " + mesg1.key);
                                Cursor cut=query(uri,null,mesg1.key,null,null);
                                return cut;
                            }
                            mesg=null;
                            //Matrix Cursor
                            String[] tmp={"key","value"};
                            mat=new MatrixCursor(tmp);
                            Log.v("NEXT","AFTER  : " + mesg1.key);
                            Log.v("NEXT K1",mesg1.key +"\n");
                            Log.v("NEXT V1",mesg1.hm+"\n");

                            Iterator it = mesg1.hm.entrySet().iterator();
                            while (it.hasNext()) {
                                Map.Entry pair = (Map.Entry) it.next();
                                //System.out.println(pair.getKey() + " = " + pair.getValue());
                                String[] str={pair.getKey().toString(),pair.getValue().toString()};
                                Log.v("NEXT K",str[0] + "\n");
                                Log.v("NEXT V",str[1] + "\n");
                                mat.addRow(str);
                            }


                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                Cursor[] out={mat};
                mgr=new MergeCursor(out);
                Log.v("Cursor","Returned");
                //Return MatrixCursor as Cursor
                //cursor= mgr;
            }
            //STARFLAG=1;
            return mgr;

        }


	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    //Generate Hash
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



    //////////////////////////////////////////////////////////////////////////////////////////////////
    //Server Task
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {


            while(true) {
                ServerSocket serverSocket = sockets[0];
                String str;
                Socket soc;


                try {

                    //Receive message
                    soc = serverSocket.accept();

                    ObjectInputStream ois = new ObjectInputStream(soc.getInputStream());
                    str = (String) ois.readObject();
                    //Convert to message format
                    if(lock.equals("true"))
                    {
                        Log.v("ACT LOCK","ENTER");
                        synchronized (act) {
                            act.wait();
                        }
                        Log.v("ACT LOCK","Exit");
                    }
                    message mess=new message();
                    mess.toJMsg(str);


                    Log.e("AA", "Client Message Sent");

                    //Type 0 Set Active status of the Receiver
                    if(mess.type.equals("0"))
                    {
                        for(int k=0;k<5;k++)
                        {
                            if(mess.key.equals(Port[k]))
                            {
                                Stat[k]=true;

                                if(!Port[k].equals(myPort))
                                {
                                    INIT=1;
                                }
                                break;
                            }
                        }
                        Log.v("MESG RECV","TYPE 0");
                        publishProgress("SEND 0", mess.key);
                        ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                        obj1.writeObject("HI");
                        obj1.flush();

                    }

                    //Type 1 Set Active status of the Sender
                    else if(mess.type.equals("1"))
                    {
                        for(int k=0;k<5;k++)
                        {
                            if(mess.key.equals(Port[k]))
                            {
                                Stat[k]=true;

                                if(!Port[k].equals(myPort))
                                {
                                    INIT=1;
                                }
                                break;
                            }
                        }
                        Log.v("MESG RECV","TYPE 1");
                        publishProgress("SEND 1", mess.key);
                        ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                        obj1.writeObject("HI");
                        obj1.flush();

                    }

                    //Type 2 Set Insert
                    else if(mess.type.equals("2"))
                    {
                       Log.v("MESG RECV","TYPE 2");
                        ContentValues cnt=new ContentValues();
                        cnt.put("key",mess.key);
                        cnt.put("value", mess.mess);
                        insertD(uri, cnt);
                        publishProgress("TYPE 2", str);
                        ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                        obj1.writeObject("HI");
                        obj1.flush();

                    }
                    else if(mess.type.equals("3"))
                    {
                        Log.v("MESG RECV","TYPE 3");
                        publishProgress("TYPE 3", str);
                        ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                        obj1.writeObject("HI");
                        obj1.flush();

                    }

                    // For *
                    else if(mess.type.equals("4")) {

                        Log.v("TYPE RECEIVED","4");
                        mesg=new message();

                        mesg=mess;


                        //If Start Port Reached
                        if(mesg.port.equals(myPort))
                        {
                            //Notify
                            Log.v("NOTIFY", myPort);
                            synchronized(TTT1){
                                TTT1.notify();
                            }
                            publishProgress("NOTIFY", "NOTIFY".valueOf(1));
                        }
                        else{

                            Log.v("NOTIFY ELSE",mesg.port);
                            query(uri, null,"#", null, null); // Send Message to next with updated DB
                            publishProgress(str, "SentNew".valueOf(1));
                        }

                        publishProgress(str, "SentNew".valueOf(1));
                        ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                        obj1.writeObject("HI");
                        obj1.flush();
                    }

                    // For Single message
                    else if(mess.type.equals("5")) {

                        Log.v("TYPE RECEIVED 55 :","5");
                        mesg3=new message();
                        //mesg=mess;
                        mesg3.port=mess.port;
                        mesg3.hm=mess.hm;
                        mesg3.key=mess.key;
                        mesg3.cnt=mess.cnt;


                        //If Start Port Reached
                        Log.v("TYPE RECEIVED 55 :","51");


                            Log.v("NOTIFY ELSE",mess.port +"\n");
                            message curm=new message();
                            curm=mesg3;
                            Cursor cur=query(uri, null,"$", null, mesg3.key); // Send Message to next with updated DB

                            curm.mess="hi";
                            curm.type="6";
                            curm.cnt="0";
                            String send="";
                        Log.v("TYPE RECEIVED 55 :","52");
                            try {
                                send=curm.toJString(cur, null);
                                Log.v("COUNT", String.valueOf(cur.getCount()));
                                Log.v("NEXT", send);
                                Log.v("START","*");
                            } catch (JSONException e) {
                                e.printStackTrace();
                            }
                        Log.v("TYPE RECEIVED 55 :","53");

                            send=send+"\n";
                            ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                            obj1.writeObject(send);
                            obj1.flush();
                            publishProgress(str, "SentNew".valueOf(1));



                    }

                    else if(mess.type.equals("7"))
                    {
                        mesg2=new message();
                        mesg2=mess;
                        Cursor cur=query(uri, null,"@", null, null); // Send Message to next with updated DB

                        mesg2.type="6";
                        mesg2.cnt=String.valueOf(cur.getCount());
                        String send="";
                        Log.v("TYPE RECEIVED 7 :","@");
                        try {
                            send=mesg2.toJString(cur, null);
                            Log.v("COUNT 7 : ", String.valueOf(cur.getCount()));
                            Log.v("NEXT 7 : ", send);
                            Log.v("START 7 : ","@");
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        Log.v("TYPE RECEIVED 7 :","7@");

                        send=send+"\n";
                        Log.v("Type 7 :",send);
                        ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                        obj1.writeObject(send);
                        obj1.flush();
                        publishProgress(str, "SentNew".valueOf(1));



                    }



                        else{

                            Log.v("NOTIFY ELSE", mess.port);

                            publishProgress(str, "SentNew".valueOf(1));
                            ObjectOutputStream obj1=new ObjectOutputStream(soc.getOutputStream());
                            obj1.writeObject("HI");
                            obj1.flush();
                        }



                    Log.e("SER", "Server Message Received");
                    soc.close();

                } catch (IOException e) {
                    Log.e("SER", "ServerSocket IOException");
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();


            if(strReceived.equals("SEND 0"))
            {
                Log.v("ACTIVE PROCESSES ", ":");
                for(int c=0;c<5;c++) {
                    if(Stat[c]) {
                        char[] tmp=Status.toCharArray();
                        tmp[c]='1';
                        Status=String.valueOf(tmp);

                    }
                }
                Log.v("PROCESS ", Status);


                // Send Pred and Succ to other processes
                new ClientTaskActive1().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "STATCALC", strings[1]);
            }

            else if(strReceived.equals("SEND 1"))
            {
                Log.v("ACTIVE PROCESSES ", ":");
                for(int c=0;c<5;c++) {
                    if(Stat[c]) {
                        char[] tmp=Status.toCharArray();
                        tmp[c]='1';
                        Status=String.valueOf(tmp);

                    }
                }
                Log.v("PROCESS ", Status);
               // PosPredSucc();
            }

            else if(strReceived.equals("TYPE 2"))
            {
                String cmp=strings[1];
                message tmp=new message();
                try {
                    tmp.toJMsg(cmp);
                } catch (JSONException e) {
                    e.printStackTrace();
                }


                tmp.type="00";



            }

            else if(strReceived.equals("NOTIFY"))
            {

                Log.v("NOTIFIED", "1");

            }


            //PosPredSucc();
            Log.v("String Received : ",strReceived);

            String filename = "GroupMessengerOutput";
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e("FILE", "File write failed");
            }

            return;
        }
    }


    ///////////


    ///////////
    //Client Task To Receive Others Activation
    private class ClientTaskActive1 extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

                try {
                    String remotePort = msgs[1];

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    message tmp = new message();
                    tmp.type = "1";
                    tmp.key = myPort;
                    tmp.port = myPort;
                    tmp.mess = "test";
                    tmp.cnt="0";
                    HashMap<String, String> ab = new HashMap<String, String>();
                    tmp.hm = ab;
                    String msgToSend = tmp.toJString();

                    ObjectOutputStream obj1=new ObjectOutputStream(socket.getOutputStream());
                    obj1.writeObject(msgToSend);
                    obj1.flush();
                    Log.e("AA", "Server Message Sent");

                    //Full-Duplex
                    InputStreamReader read = new InputStreamReader(socket.getInputStream());
                    BufferedReader bread = new BufferedReader(read);


                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e("AA", "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e("AA", "ClientTask socket IOException");
                    Log.v("PORT","NOT ACTIVE");
                } catch (JSONException e) {
                    e.printStackTrace();
                }


            return null;
        }
    }


    ///////////
    //Client Task To Receive Others Activation
    private class ClientTaskDirect extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            int ctr=0;
            message tmpa=new message();
            try {
                tmpa.toJMsg(msgs[0]);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            int i;
            for(i=0;i<5;i++)
            {
                if(Port[i].equals(tmpa.port))
                {
                    break;
                }
            }

            String [] ppt=new String[3];
            ppt[0]=Port[i];
            while(ctr<2)
            {
                i++;
                if(i>4)
                {
                    i=0;
                }
                if(Stat[i])
                {
                    ctr++;
                    ppt[ctr]=Port[i];
                    }

            }

            for(i=0;i<3;i++) {
                try {

                        String remotePort = ppt[i];

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));

                        String msgToSend = msgs[0];

                    ObjectOutputStream obj1=new ObjectOutputStream(socket.getOutputStream());
                    obj1.writeObject(msgToSend);
                    obj1.flush();
                        Log.e("AA", "Server Message Sent");

                    //Full-Duplex
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    //socket.setSoTimeout(1500);
                    String strr= (String) ois.readObject();



                        socket.close();

                } catch (UnknownHostException e) {
                    Log.e("AA", "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e("AA", "ClientTask socket IOException");
                    Log.v("PORT", "NOT ACTIVE");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            synchronized (LOCK) {

                queryLock = 0;
                LOCK.notify();
            }

            return null;
        }
    }

    ///////////
    //Client Task To Receive Others Activation
    private class ClientTaskReplicate extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            message tmp=new message();


            try {

                tmp.toJMsg(msgs[0]);
                String remotePort = succ;

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                String msgToSend = msgs[0];


                    ObjectOutputStream obj1 = new ObjectOutputStream(socket.getOutputStream());
                    obj1.writeObject(msgToSend);
                    obj1.flush();
                    Log.e("AA Replicate", "Server Message Sent to " + succ);

                //Full-Duplex
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                //socket.setSoTimeout(1500);
                String strr= (String) ois.readObject();

                Log.v("AA REP",strr);

                socket.close();

            }
            catch(SocketTimeoutException e2)
            {
                Log.e("TIME OUT","EXCEPTION");
            }

            catch (IOException e) {
                Log.e("AA Replicate", "ClientTask socket IOException");
                Log.v("PORT", "NOT ACTIVE");

                try {
                    tmp.toJMsg(msgs[0]);
                    int a=POS+1;
                    if(a>4)
                           a=0;
                    a=a+1;
                    if(a>4)
                        a=0;

                String remotePort = Port[a];

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                String msgToSend = msgs[0];

                    ObjectOutputStream obj1=new ObjectOutputStream(socket.getOutputStream());
                    obj1.writeObject(msgToSend);
                    obj1.flush();
                Log.e("AA", "Server Message Sent to " + remotePort);

                //Full-Duplex
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                   // socket.setSoTimeout(1500);
                    String strr= (String) ois.readObject();
                    Log.v("AA REP",strr);


                socket.close();
                } catch (JSONException e1) {
                    e1.printStackTrace();
                } catch (UnknownHostException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (ClassNotFoundException e1) {
                    e1.printStackTrace();
                }


            }
            catch (JSONException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }


            return null;
        }
    }


    ///////////
    //Client Task To Send to Specific Port
    private class ClientTaskSendTo extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Log.v("SEND ","TO");

            synchronized (LOCK) {
                if (queryLock == 1 && lock.equals("false")) {
                    try {
                        LOCK.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                message tmp=new message();
                tmp.toJMsg(msgs[0]);

                String remotePort = tmp.mess;

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                String msgToSend = msgs[0];

                ObjectOutputStream obj1=new ObjectOutputStream(socket.getOutputStream());
                obj1.writeObject(msgToSend);
                obj1.flush();
                Log.e("AA", "Server Message Sent");

                //Full-Duplex
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                //socket.setSoTimeout(1500);
                String str= (String) ois.readObject();
                mesg=new message();
                //Convert to message format

                Log.v("AA MSG", str + "");
                if(str!=null)
                    mesg.toJMsg(str);

                Log.v("AA", "Server Message Received");


                socket.close();
                synchronized(TTT)
                {
                    Log.v("Current HM for "+ mesg.key+" : ",mesg.hm.toString());
                    TTT.notify();
                }
                Log.v("NOTIFIED", str+"\n");
                Log.v("NOTIFIED", "1");



            }  catch (IOException e) {
                Log.e("AA NOTIFY", "ClientTask socket IOException");
                Log.v("PORT","NOT ACTIVE");


                try{
                    if(msgs[1].equals("2")) {

                        message tmp = new message();
                        tmp.toJMsg(msgs[0]);

                        int i = 0;
                        for (i = 0; i < 4; i++) {
                            if (tmp.mess.equals(Port[i]))
                                break;
                        }

                        int a = i + 1;
                        if (a > 4)
                            a = 0;


                        String remotePort = Port[a];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));

                        String msgToSend = msgs[0];

                        ObjectOutputStream obj1 = new ObjectOutputStream(socket.getOutputStream());
                        obj1.writeObject(msgToSend);
                        obj1.flush();
                        Log.e("AA", "Server Message Sent");

                        //Full-Duplex
                        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                        //socket.setSoTimeout(1500);
                        String str = (String) ois.readObject();
                        mesg = new message();
                        //Convert to message format

                        Log.v("AA MSG", str + "");
                        if (str != null)
                            mesg.toJMsg(str);

                        Log.v("AA", "Server Message Received");


                        socket.close();
                        synchronized (TTT) {
                            Log.v("Current HM : ", mesg.hm.toString());
                            TTT.notify();
                        }
                        Log.v("NOTIFIED", str + "\n");
                        Log.v("NOTIFIED", "2");
                    }
                    else if(msgs[1].equals("7"))
                    {

                        synchronized (TTT) {
                            mesg=new message();
                            Log.v("Notify","7");
                            TTT.notify();
                        }
                    }

                } catch (UnknownHostException e1) {
                    e1.printStackTrace();
                } catch (IOException e1) {
                    e1.printStackTrace();
                } catch (JSONException e1) {
                    e1.printStackTrace();
                } catch (ClassNotFoundException e1) {
                    e1.printStackTrace();
                }


            }catch (JSONException e) {
                e.printStackTrace();
                Log.e("JSON","EXEP");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }


            return null;
        }
    }
}
