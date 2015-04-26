package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static Context context;
    private String[] portNumbers = {"5562", "5556", "5554", "5558", "5560"};
    static final int SERVER_PORT = 10000;
    static final String KEY_FIELD = "key";
    static final String VALUE_FIELD = "value";
    String portStr = null;
    String finalFetchedQuery=null;
    String myPort = null;
    String firstSuccesor = null;
    String secondSuccessor = null;
    String predecessor = null;
    public Uri mUri = null;
    Boolean waitFlag=false;
    Boolean queryWaitFlag=false;
    Boolean deleteWaitFlag=false;
    Boolean starDeleteWaitFlag=false;
    Boolean starQueryWaitFlag=false;
    List<String> deleteList=null;
    List<String> acknList=null;
    List<String> starQueryList=null;
    public static Map<String,String> ringMap=new TreeMap<String,String>();
    public Map<String,String> keyValueMap=null;
    //buildUri method for content provider
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    //Delete Section Started
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String holderPort=null;
        String selectionQuery = selection.replaceAll("\"", "");
        if (selectionQuery.equals("@")) {
            deleteAll();
            return 1;
        } else if ((!selectionQuery.equals("@") && !selectionQuery.equals("*"))) {
            deleteWaitFlag=true;
            for (String str : portNumbers) {
                boolean flag = containsRequest(selectionQuery, str);
                if (flag) {
                    holderPort=str;
                }
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE),holderPort,selectionQuery);
            Log.d(TAG,"value of deleteWaitFlag before waiting"+deleteWaitFlag);
            while (deleteWaitFlag);
            Log.d(TAG,"value of deleteWaitFlag after waiting"+deleteWaitFlag);
            return 1;
        }
        else if(selectionQuery.equals("*")){
            starDeleteWaitFlag=true;
            deleteList=new ArrayList<String>();
            for (String str : portNumbers) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETESTAR),str);
            }
            Log.d(TAG,"starDeleteWaitFlag before waiting"+String.valueOf(starDeleteWaitFlag));
            while (starDeleteWaitFlag);
            Log.d(TAG,"starDeleteWaitFlag after waiting"+String.valueOf(starDeleteWaitFlag));
            Log.d(TAG,"Delete Successful");
                //send everyone @ parameter for delete
        }
        return 0;

    }

    //deleting all the entries in the content provider.
    public int deleteAll() {
        try {
            for (String str : getContext().fileList()) {
                getContext().deleteFile(str);
            }
            return 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    //Deleting a particular record
    public int deleteRecord(String key) {
        try {
            getContext().deleteFile(key);
            return 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        waitFlag=true;
        acknList=new ArrayList<String>();
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        Log.d("InsideInsertMethodKey", key);
        Log.d("InsideInsertMethodValue", value);
        String holderPort = null;
        String[] successors = null;
        String succesor1Port = null;
        String succesor2Port = null;
        try {
            for (String str : portNumbers) {
                boolean flag = containsRequest(key, str);
                if (flag) {
                    holderPort = str;
                    successors = getSuccessors(holderPort);
                    break;
                }
                succesor1Port = successors[0];
                succesor2Port = successors[1];
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.HOLDERINSERTMODE),holderPort,key,value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SUCCESSOR1MODE),succesor1Port,key,value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SUCCESSOR2MODE),succesor2Port,key,value);
            Log.d(TAG,"WaitFlag before waiting"+String.valueOf(waitFlag));
            while(waitFlag);
            Log.d(TAG,"WaitFlag after waiting"+String.valueOf(waitFlag));
            Log.d(TAG,"Insert Successful");

        } catch (Exception e) {
            e.printStackTrace();
        }
        Log.v("insert", value.toString());
        return uri;
    }

    public boolean insertRecord(String key, String value) {
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(key, getContext().MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
            Log.d(TAG, "Insert Record called with key-->" + key);
            return true;
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
        return false;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            ringMap.put(genHash("5562"), "5562");
            ringMap.put(genHash("5556"), "5556");
            ringMap.put(genHash("5554"), "5554");
            ringMap.put(genHash("5558"), "5558");
            ringMap.put(genHash("5560"), "5560");
        }
        catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.SimpleDynamoProvider.provider");
        Log.d(TAG, "--->Inside OnCreateMethod");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(getContext().TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            //Getting the value of two successors for a given port.
            String[] successors = getSuccessors(portStr);
            firstSuccesor = successors[0];
            secondSuccessor = successors[1];
            predecessor = getPredecessor(portStr);
            Log.d(TAG, "My port number is-->" + myPort);
            Log.d(TAG, "My AVD number is-->" + portStr);
            Log.d(TAG, "firstSuccessor is-->" + firstSuccesor);
            Log.d(TAG, "secondSuccessor is-->" + secondSuccessor);
            Log.d(TAG, "My predecessor is-->" + predecessor);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Executor e = Executors.newFixedThreadPool(10);
            new ServerTask().executeOnExecutor(e, serverSocket);
            // new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return false;
    }
    public String queryRecord(String key)
    {
        FileInputStream inputStream = null;
        MatrixCursor matrixCursor = null;
        BufferedReader bufferedReader = null;
        InputStreamReader inputStreamReader = null;
        try {
            inputStream = getContext().openFileInput(key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        inputStreamReader = new InputStreamReader(inputStream);
        bufferedReader = new BufferedReader(inputStreamReader);
        String value = null;
        try {
            value = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return key+"%"+value;
    }

    public void insertQueryStringIntoMap(String queryString){
        if(queryString!=null && queryString!="") {
            String str = queryString.substring(0, queryString.length());
            String[] result = str.split("%");
            for (int i = 0; i < result.length; i++) {
                String[] s = result[i].split("=");
                String key = s[0];
                Log.d("key in insertQueryStringIntoMap", key);
                String value = s[1];
                Log.d("key in insertQueryStringIntoMap", value);
                keyValueMap.put(key, value);
            }
        }
    }
    public String getQueryString() {
        FileInputStream inputStream = null;
        MatrixCursor matrixCursor = null;
        BufferedReader bufferedReader = null;
        InputStreamReader inputStreamReader = null;
        String query = "";
        for (String str : getContext().fileList()) {
            try {
                inputStream = getContext().openFileInput(str);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            inputStreamReader = new InputStreamReader(inputStream);
            bufferedReader = new BufferedReader(inputStreamReader);
            String key = str;
            try {
                String value = bufferedReader.readLine();
                query = query +key +"="+value+"%";
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return query;
    }

    public boolean containsRequest(String query, String portNo) {
        boolean flag = false;
        try {
            Log.d("Inside contains Request", "Query-->" + query + " " + "Predecessor->=" + predecessor + " " + "client->" + portNo + " " + "Successor->" + predecessor);
            if (genHash(predecessor).compareTo(genHash(portNo)) > 0 && (((genHash(query).compareTo(genHash(portNo)) < 0) || (genHash(query).compareTo(genHash(predecessor)) > 0))))
                flag = true;
            else if (genHash(query).compareTo(genHash(predecessor)) > 0 && genHash(query).compareTo(genHash(portNo)) <= 0)
                flag = true;
            else if (predecessor.equals(portNo) && portNo.equals(predecessor))
                flag = true;
            else
                flag = false;
            Log.d("Inside contains Request", String.valueOf(flag));
            return flag;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }


    public String[] getSuccessors(String portStr) {
        String[] successors=new String[2];
        ArrayList<String> keyList=new ArrayList<String>(ringMap.values());
        int index=keyList.indexOf(portStr);
        int count=0;
        for(int i=1;i<keyList.size();i++){
            if(count==2){
                break;
            }
            successors[count]=keyList.get((index+i)%keyList.size());
            count++;
        }
        return successors;
    }

    public String getPredecessor(String portStr) {
        String predecessor=null;
        ArrayList<String> keyList=new ArrayList<String>(ringMap.values());
        int index=keyList.indexOf(portStr);
        int count=0;
        if(index==0){
            predecessor=keyList.get(keyList.size()-1);
        }
        else {
            for (int i = 1; i < keyList.size(); i++) {
                if (count == 1) {
                    break;
                }
                predecessor = keyList.get((index - i) % keyList.size());
                count++;
            }
        }
        return predecessor;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        FileInputStream inputStream = null;
        MatrixCursor matrixCursor = null;
        BufferedReader bufferedReader = null;
        InputStreamReader inputStreamReader = null;
        String queryHolderPort = null;
        Log.d("selection",selection);
        String selectionQuery= selection.replaceAll("\"","");
        Log.d("selectionQuery",selectionQuery);
        if((!selectionQuery.equals("@") && !selectionQuery.equals("*"))){
            for (String str : portNumbers) {
            boolean flag = containsRequest(selectionQuery, str);
            if (flag) {
                queryHolderPort = str;
                Log.d(TAG,"The port where key belongs is"+queryHolderPort);
                break;
            }
        }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY),queryHolderPort,selectionQuery);
            queryWaitFlag=true;
            Log.d(TAG,"queryWaitFlag before waiting"+String.valueOf(queryWaitFlag));
            while(queryWaitFlag);
            Log.d(TAG,"queryWaitFlag after waiting"+String.valueOf(queryWaitFlag));
            String key=finalFetchedQuery.split("%")[0];
            String value=finalFetchedQuery.split("%")[1];
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            matrixCursor.newRow().add(key).add(value);
            return matrixCursor;
        }
        else if (selectionQuery.equals("@")) {
            Log.d(TAG, "Inside Query @ method");
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            for (String str : getContext().fileList()) {
                try {
                    inputStream = getContext().openFileInput(str);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                inputStreamReader = new InputStreamReader(inputStream);
                bufferedReader = new BufferedReader(inputStreamReader);
                String key = str;
                String value = null;
                try {
                    value = bufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                matrixCursor.newRow().add(key).add(value);
            }
            return matrixCursor;
        }
        else if (selectionQuery.equals("*")) {
            starQueryWaitFlag=true;
            keyValueMap=new HashMap<String,String>();
            starQueryList=new ArrayList<String>();
            for (String str : portNumbers) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.STARQUERY),str);
            }
            Log.d(TAG,"starQueryWaitFlag before waiting"+String.valueOf(starQueryWaitFlag));
            while(starQueryWaitFlag);
            Log.d(TAG,"starQueryWaitFlag after waiting"+String.valueOf(starQueryWaitFlag));
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            for(String key:keyValueMap.keySet()){
                matrixCursor.newRow().add(key).add(keyValueMap.get(key));
            }
            return matrixCursor;
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket client = null;
            BufferedReader in = null;
            PrintStream ps=null;
            Log.d("ServerAccept", "ServerAccept");
            while (true) {
                try {
                    client = serverSocket.accept();
                    if (client.isConnected()) {
                        Log.d("server", "resumed");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "Exception occurred in accepting Connection");
                }
                if (client != null) try {
                    in = new BufferedReader(
                            new InputStreamReader(client.getInputStream()));
                } catch (IOException e) {
                    Log.e(TAG, "Exception occurred in creating Inputstream");
                    e.printStackTrace();
                }
                if (in != null) {
                    String strings = null;
                    try {
                        try {
                            strings = in.readLine();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        Log.d(TAG, "Incoming String in Server-->" + strings);
                        String modeString = strings.split("~")[0];
                        Log.d(TAG, "Mode String in Server-->" + modeString);
                        Mode mode = Mode.valueOf(modeString);
                        switch (mode) {
                            case HOLDERINSERTMODE:
                                Log.d(TAG, "Inside HOLDERINSERTMODE in server");
                                String key=strings.split("~")[1];
                                String value=strings.split("~")[2];
                                insertRecord(key,value);
                                 ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("holderMessageSaved");
                                ps.flush();
                            case SUCCESSOR1MODE:
                                Log.d(TAG, "Inside SUCCESSOR1MODE in server");
                                String key1=strings.split("~")[1];
                                String value1=strings.split("~")[2];
                                insertRecord(key1,value1);
                                 ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("successor1MessageSaved");
                                ps.flush();
                            case SUCCESSOR2MODE:
                                Log.d(TAG, "Inside SUCCESSOR2MODE in server");
                                String key2=strings.split("~")[1];
                                String value2=strings.split("~")[2];
                                insertRecord(key2,value2);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("successor2MessageSaved");
                                ps.flush();
                            case QUERY:
                                Log.d(TAG, "Inside QUERYMODE in server");
                                String selectionQuery=strings.split("~")[1];
                                String fetchedQuery=queryRecord(selectionQuery);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println(fetchedQuery);
                                ps.flush();
                            case STARQUERY:
                                Log.d(TAG, "Inside STARQUERYMODE in server");
                                String queryString=getQueryString();
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println(queryString);
                                ps.flush();
                            case DELETE:
                                Log.d(TAG, "Inside DELETEMODE in server");
                                String selectionQueryDelete=strings.split("~")[1];
                                deleteRecord(selectionQueryDelete);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("recordDeleted");
                                ps.flush();
                            case DELETESTAR:
                                Log.d(TAG, "Inside DELETESTARMODE in server");
                                deleteAll();
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("DeletedAll");
                                ps.flush();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        }
    }
    public void sendRequest(String str, int portNumber) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNumber);
            if (socket.isConnected()) {
                PrintStream ps = new PrintStream
                        (socket.getOutputStream());
                ps.println(str);
                ps.flush();
                ps.close();
                socket.close();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String modeString = (String) msgs[0];
            Mode mode = Mode.valueOf(modeString);
            BufferedReader in = null;
            Log.d(TAG, "Mode value in Client Side" + modeString);
            switch (mode) {

                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.INSERTMODE), holderPort, succesor1Port, succesor2Port, key, value);
                case HOLDERINSERTMODE:
                            Log.d(TAG, "Inside HOLDERINSERTMODE in client");
                            String holderPort = msgs[1];
                            String key = msgs[2];
                            String value = msgs[3];
                            String holderPortNumber = String.valueOf((Integer.parseInt(holderPort) * 2));
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(holderPortNumber));
                                //socket.setSoTimeout(10000);
                                if (socket.isConnected()) {
                                    PrintStream ps = new PrintStream
                                            (socket.getOutputStream());
                                    ps.println(String.valueOf(Mode.HOLDERINSERTMODE)+"~"+key+"~"+value);
                                    ps.flush();
                                    //ps.close();
                                    //check socket time out before closing the socket.
                                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                    String msg=in.readLine();
                                    if(msg.equals("holderMessageSaved"))
                                        acknList.add("holderDone");
                                    Log.d(TAG,"AckList is-->"+acknList.toString());
                                    if(acknList.size()==3){
                                        waitFlag=false;
                                        Log.d(TAG, "waitflag condition false in HOLDERINSERTMODE");
                                    }
                                    socket.close();
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            break;
                   case SUCCESSOR1MODE:
                            Log.d(TAG, "Inside SUCCESSOR1MODE in client");
                            String successor1Port = msgs[1];
                            String key1 = msgs[2];
                            String  value1 = msgs[3];
                            String successor1PortNumber = String.valueOf((Integer.parseInt(successor1Port) * 2));
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor1PortNumber));
                                //socket.setSoTimeout(10000);
                                if (socket.isConnected()) {
                                    PrintStream ps = new PrintStream
                                            (socket.getOutputStream());
                                    ps.println(String.valueOf(Mode.SUCCESSOR1MODE)+"~"+key1+"~"+value1);
                                    ps.flush();
                                   // ps.close();
                                    //check socket time out before closing the socket.
                                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                    String msg=in.readLine();
                                    if(msg.equals("successor1MessageSaved"))
                                        acknList.add("successor1Done");
                                    Log.d(TAG,"AckList is-->"+acknList.toString());
                                    if(acknList.size()==3){
                                        waitFlag=false;
                                        Log.d(TAG, "waitflag condition false in SUCCESSOR1MODE");
                                    }
                                    socket.close();
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                       break;
                case SUCCESSOR2MODE:
                    Log.d(TAG, "Inside SUCCESSOR2MODE in client");
                    String successor2Port = msgs[1];
                    String key2 = msgs[2];
                    String value2 = msgs[3];
                    String successor2PortNumber = String.valueOf((Integer.parseInt(successor2Port) * 2));
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor2PortNumber));
                        //  socket.setSoTimeout(10000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.SUCCESSOR2MODE)+"~"+key2+"~"+value2);
                            ps.flush();
                           // ps.close();
                            //check socket time out before closing the socket.
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String msg=in.readLine();
                            if(msg.equals("successor2MessageSaved"))
                                acknList.add("successor2Done");
                            Log.d(TAG,"AckList is-->"+acknList.toString());
                            if(acknList.size()==3){
                                waitFlag=false;
                                Log.d(TAG, "waitflag condition false in SUCCESSOR2MODE");
                            }
                            socket.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                   // new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY),queryHolderPort,selectionQuery);
                case QUERY:
                    Log.d(TAG, "Inside QUERYMODE in client");
                    String queryHolderPort = msgs[1];
                    String selectionQuery = msgs[2];
                    String queryHolderPortNumber = String.valueOf((Integer.parseInt(queryHolderPort) * 2));
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(queryHolderPortNumber));
                        //  socket.setSoTimeout(10000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.QUERY)+"~"+selectionQuery);
                            ps.flush();
                            // ps.close();
                            //check socket time out before closing the socket.
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            finalFetchedQuery=in.readLine();
                            Log.d(TAG,"final query retrieved is"+finalFetchedQuery);
                            queryWaitFlag=false;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                // new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.STARQUERY),str);
                case STARQUERY:
                    Log.d(TAG, "Inside STARQUERYMODE in client");
                    String port=msgs[1];
                    String portNumber = String.valueOf((Integer.parseInt(port) * 2));
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNumber));
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.STARQUERY));
                            ps.flush();
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String queryString=in.readLine();
                            insertQueryStringIntoMap(queryString);
                            starQueryList.add("All keys added for"+port);
                            if(starQueryList.size()==5){
                                starQueryWaitFlag=false;
                            }
                        }
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
                case DELETE:
                    Log.d(TAG, "Inside DELETEMODE in client");
                    String holderPortDelete=msgs[1];
                    String selectionQueryDelete = msgs[2];
                    String holderPortDeleteNumber = String.valueOf((Integer.parseInt(holderPortDelete) * 2));
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(holderPortDeleteNumber));
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.DELETE)+"~"+selectionQueryDelete);
                            ps.flush();
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String msg=in.readLine();
                            if(msg.equals("recordDeleted")){
                                deleteWaitFlag=false;
                            }
                        }
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
                case DELETESTAR:
                    Log.d(TAG, "Inside DELETESTARMODE in client");
                    String starholderPortDelete=msgs[1];
                    String starholderPortDeleteNumber = String.valueOf((Integer.parseInt(starholderPortDelete) * 2));
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(starholderPortDeleteNumber));
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.DELETESTAR));
                            ps.flush();
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String msg=in.readLine();
                            if(msg.equals("DeletedAll")){
                                deleteList.add("All keys deleted for"+starholderPortDelete);
                                if(deleteList.size()==5){
                                    deleteWaitFlag=false;
                                }

                            }
                        }
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
            }
            return null;
        }
    }
    public enum Mode {
        HOLDERINSERTMODE,SUCCESSOR1MODE,SUCCESSOR2MODE,QUERY,STARQUERY,DELETE,DELETESTAR
    }
}
