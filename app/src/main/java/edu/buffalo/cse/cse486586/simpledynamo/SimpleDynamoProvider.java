package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.nfc.Tag;
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
    String failedNode=null;
    String portStr = null;
    String finalFetchedQuery=null;
    String myPort = null;
    String firstSuccesor = null;
    String secondSuccessor = null;
    String predecessor = null;
    public Uri mUri = null;
    Boolean waitFlag=false;
    Boolean queryWaitFlag=false;
    Boolean syncWaitFlag=false;
    Boolean deleteWaitFlag=false;
    Boolean starDeleteWaitFlag=false;
    Boolean starQueryWaitFlag=false;
    Boolean nodeIsUpFlag=false;
    Boolean failureFlag=false;
    Boolean syncFlag=false;
    List<String> deleteStarList=null;
    // List<String> deleteList=null;
    List<String> acknList=null;
    List<String> starQueryList=null;
    public static Map<String,Boolean> waitMap=new ConcurrentHashMap<String,Boolean>();
    public static Map<String,List<String>> insertWaitMap=new ConcurrentHashMap<String,List<String>>();
    public static Map<String,String> ringMap=new TreeMap<String,String>();
    public static Map<String,String> genHashMap=new HashMap<>();
    public Map<String,String> keyValueMap=null;
    public List<String> syncList=null;
    public Map<String,String> syncMap=null;
    public Map<String,String> syncMapSuccessor=null;
    public Map<String,String> syncMapPredecessor=null;

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
        String[] successors = null;
        String succesor1Port = null;
        String succesor2Port = null;
        String selectionQuery = selection.replaceAll("\"", "");
        if (selectionQuery.equals("@")) {
            deleteAllSync();
            return 1;
        } else if ((!selectionQuery.equals("@") && !selectionQuery.equals("*"))) {
            //deleteList=new ArrayList<String>();
            deleteWaitFlag=true;
            for (String str : portNumbers) {
                String incomingPredecessor=getPredecessor(str);
                boolean flag = containsRequest(selectionQuery, str,incomingPredecessor);
                if (flag) {
                    holderPort=str;
                }
            }
            successors = getSuccessors(holderPort);
            succesor1Port = successors[0];
            succesor2Port = successors[1];
            Log.d(TAG,"Successors of the holder in delete are-->"+succesor1Port+","+succesor2Port);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE),holderPort,selectionQuery);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE),succesor1Port,selectionQuery);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.DELETE),succesor2Port,selectionQuery);
            //Log.d(TAG,"value of deleteWaitFlag before waiting"+deleteWaitFlag);
            // while (deleteWaitFlag);
            // Log.d(TAG,"value of deleteWaitFlag after waiting"+deleteWaitFlag);
            return 1;
        }
        else if(selectionQuery.equals("*")){
            starDeleteWaitFlag=true;
            deleteStarList=new ArrayList<String>();
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
    public int deleteAllSync() {
        try {
            for (String str : getContext().fileList()) {
                if(!str.equals("dummyRecord")) {
                    getContext().deleteFile(str);
                }
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
        // waitFlag=true;
        Log.d(TAG,"Value of syncFlag in Insert"+String.valueOf(syncFlag));
        while (syncFlag);
        Log.d(TAG,"Inside Insert Method");
        acknList=new ArrayList<String>();
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        insertWaitMap.put(key+"$"+portStr,acknList);
        Log.d("InsideInsertMethodKey", key);
        Log.d("InsideInsertMethodValue", value);
        String holderPort = null;
        String[] successors = null;
        String succesor1Port = null;
        String succesor2Port = null;
        try {
            for (String str : portNumbers) {
                String incomingPredecessor=getPredecessor(str);
                boolean flag = containsRequest(key, str,incomingPredecessor);
                if (flag) {
                    holderPort = str;
                    Log.d(TAG,"Key belongs to-->"+holderPort);
                    break;
                }
            }
            successors = getSuccessors(holderPort);
            succesor1Port = successors[0];
            succesor2Port = successors[1];
            Log.d(TAG,"Successors of the holder are-->"+succesor1Port+","+succesor2Port);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.HOLDERINSERTMODE),holderPort,key,value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SUCCESSOR1MODE),succesor1Port,key,value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SUCCESSOR2MODE),succesor2Port,key,value);
            Log.d(TAG,"WaitFlag before waiting"+insertWaitMap.get(key+"$"+portStr));
            while ((insertWaitMap.get(key+"$"+portStr).size()<2));
            Log.d(TAG,"WaitFlag after waiting"+insertWaitMap.get(key+"$"+portStr));
            Log.d(TAG,"Insert Successful");
            // Log.d(TAG,"WaitFlag before waiting"+String.valueOf(waitFlag));
            // while(acknList.size()<2);
            //Log.d(TAG,"WaitFlag after waiting"+String.valueOf(waitFlag));
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
    public boolean fileExist(String fname){
        File file = getContext().getFileStreamPath(fname);
        return file.exists();
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
            genHashMap.put("5562","177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
            genHashMap.put("5556","208f7f72b198dadd244e61801abe1ec3a4857bc9");
            genHashMap.put("5554","33d6357cfaaf0f72991b0ecd8c56da066613c089");
            genHashMap.put("5558","abf0fd8db03e5ecb199a9b82929e9db79b909643");
            genHashMap.put("5560","c25ddd596aa7c81fa12378fa725f706d54325d12");
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
            if(fileExist("dummyRecord")){
                nodeIsUpFlag=true;
                syncFlag=true;
                Log.d(TAG, "Inside FileExists Condition");
            }
            if(nodeIsUpFlag){
                deleteAllSync();
                String[] successors = getSuccessors(portStr);
                String[] predecessors=getFirstSecondPredecessor(portStr);
                Log.d(TAG,"Value of syncFlag"+String.valueOf(syncFlag));
                syncList=new ArrayList<>();
                syncRequest(successors,predecessors);
                Log.d(TAG, "Inside nodeIsUp Condition");
            }
            if(!fileExist("dummyRecord")){
                insertRecord("dummyRecord","dummyValue");
            }
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

    public void insertSyncMapIntoContentProvider(Map<String,String> incomingMap){
        Log.d(TAG,"insertSyncMapIntoContentProvider called");
        for(String str:incomingMap.keySet()){
            insertRecord(str,incomingMap.get(str));
        }
        Log.d(TAG,"Insert pahse 1 complete");
    }
    public void syncRequest(String[] successors,String[] predecessors)
    {
        //To do in case the node is back up again.
        String firstSuccessor=successors[0];
        String secondSuccessor=successors[1];
        String firstPredecessor=predecessors[0];
        String secondPredecessor=predecessors[1];
        Log.d(TAG, "Inside syncRequest");
        Log.d(TAG, "firstSuccessor is-->" + firstSuccessor);
        Log.d(TAG, "secondSuccessor is-->" + secondSuccessor);
        Log.d(TAG, "My firstPredecessor is-->" + firstPredecessor);
        Log.d(TAG, "My secondPredecessor is-->" + secondPredecessor);
        Log.d(TAG,"Vaue of syncFlag in syncRequest"+String.valueOf(syncFlag));
        // for(String str:successors){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SYNCSUCCESSORS),firstSuccessor);
        // }
        for(String str1:predecessors){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SYNCPREDECESSORS),str1);
        }
        Log.d(TAG,"Synch Complete");
        //To ask my two successors and two predecessors for values
    }
    public String queryRecord(String incomingKey)
    {
        FileInputStream inputStream = null;
        MatrixCursor matrixCursor = null;
        BufferedReader bufferedReader = null;
        InputStreamReader inputStreamReader = null;
        try {
            inputStream = getContext().openFileInput(incomingKey);
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
        return incomingKey + "%" + value;
    }


    public void insertQueryStringIntoMap(String queryString){
        if(queryString!=null && queryString!="") {
            String str = queryString.substring(0, queryString.length());
            String[] result = str.split("%");
            for (int i = 0; i < result.length; i++) {
                String[] s = result[i].split("=");
                String key = s[0];
                Log.d(TAG,"key in insertQueryStringIntoMap-->"+key);
                String value = s[1];
                Log.d(TAG,"key in insertQueryStringIntoMap--->"+value);
                keyValueMap.put(key, value);
            }
        }
    }
    public void insertQueryStringIntoSyncMap(String queryString,Map<String,String> incomingMap){
        Log.d(TAG,"insertQueryStringIntoSyncMap called");
        if(queryString!=null && queryString!="") {
            String str = queryString.substring(0, queryString.length());
            String[] result = str.split("%");
            for (int i = 0; i < result.length; i++) {
                String[] s = result[i].split("=");
                String key = s[0];
                Log.d(TAG,"key in insertQueryStringIntoSyncMap-->"+key);
                String value = s[1];
                Log.d(TAG,"value in insertQueryStringIntoSyncMap-->"+value);
                incomingMap.put(key, value);
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


    public String getQueryStringSuccessor(String incomingPortNumber) {
        //In case of successor the incomingPortNumber will be of the node that has recovered.
        //In case of preDecessor the incomingPort will be of the node that will be of predecessor itself.
        Log.d(TAG,"getQueryStringSuccessor called from servers");
        FileInputStream inputStream = null;
        MatrixCursor matrixCursor = null;
        BufferedReader bufferedReader = null;
        InputStreamReader inputStreamReader = null;
        String query = "";
        for (String str : getContext().fileList()) {
            String predecessor = getPredecessor(incomingPortNumber);
            if (containsRequest(str, incomingPortNumber, predecessor)) {
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
                    query = query + key + "=" + value + "%";
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return query;
    }

    public boolean containsRequest(String query, String portNo,String incomingPredecessor) {
        boolean flag = false;
        try {
            Log.d("Inside contains Request", "Query-->" + query + " " + "Predecessor->=" + incomingPredecessor + " " + "client->" + portNo);
            if (genHash(incomingPredecessor).compareTo(genHashMap.get(portNo)) > 0 && (((genHash(query).compareTo(genHashMap.get(portNo)) < 0) || (genHash(query).compareTo(genHashMap.get(incomingPredecessor)) > 0))))
                flag = true;
            else if (genHash(query).compareTo(genHashMap.get(incomingPredecessor)) > 0 && genHash(query).compareTo(genHashMap.get(portNo)) <= 0)
                flag = true;
            else if (incomingPredecessor.equals(portNo) && portNo.equals(incomingPredecessor))
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


    public String[] getFirstSecondPredecessor(String portStr) {
        String[] predecessor = new String[2];
        ArrayList<String> keyList = new ArrayList<String>(ringMap.values());
        int index = keyList.indexOf(portStr);
        int count = 0;
        for (int i = 1; i < keyList.size(); i++) {
            if (count == 2) {
                break;
            }
            predecessor[count] = keyList.get(((index - i) + keyList.size()) % keyList.size());
            count++;
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
        Log.d(TAG,"Value of syncFlag in query"+String.valueOf(syncFlag));
        while (syncFlag);
        if ((!selectionQuery.equals("@") && !selectionQuery.equals("*"))) {
            //queryWaitFlag = true;
            for (String str : portNumbers) {
                String incomingPredecessor = getPredecessor(str);
                boolean flag = containsRequest(selectionQuery, str, incomingPredecessor);
                if (flag) {
                    queryHolderPort = str;
                    Log.d(TAG, "The port where key belongs is" + queryHolderPort);
                    break;
                }
            }
            Log.d(TAG, "Map condition true for" + queryHolderPort + "$" + selectionQuery);
            //To think whether to make a new map object everytime?
            //Its the case where two concurrent operations take place at the same node for the same key.
            waitMap.put(queryHolderPort + "$" + selectionQuery, true);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY), queryHolderPort, selectionQuery);
            // Log.d(TAG, "queryWaitFlag before waiting" + String.valueOf(queryWaitFlag));
            Log.d(TAG,"SYNCFLAG VALUE Before Waiting"+String.valueOf(syncFlag));
            while ((waitMap.get(queryHolderPort + "$" + selectionQuery))) ;
            Log.d(TAG,"SYNCFLAG VALUE After Waiting"+String.valueOf(syncFlag));
            //  Log.d(TAG, "queryWaitFlag after waiting" + String.valueOf(queryWaitFlag));
            Log.d(TAG, "FinalFetchedQuery in individual query" + finalFetchedQuery);
            if (finalFetchedQuery != null && finalFetchedQuery != "") {
                Log.d(TAG, "Inside FinalFetchedQuery is not null");
                String key = finalFetchedQuery.split("%")[0];
                String value = finalFetchedQuery.split("%")[1];
                matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                matrixCursor.newRow().add(key).add(value);
                return matrixCursor;
            }

        }
        if (selectionQuery.equals("@")) {
            Log.d(TAG, "Inside Query @ method");
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            for (String str : getContext().fileList()) {
                if(!str.equals("dummyRecord")) {
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
            }
            return matrixCursor;
        }
        if (selectionQuery.equals("*")) {
           /* if(fileExist("dummyRecord")){
                deleteRecord("dummyRecord");
                Log.d(TAG,"dummy record deleted in *");
            }*/
            starQueryWaitFlag = true;
            keyValueMap = new HashMap<String, String>();
            starQueryList = new ArrayList<String>();
            for (String str : portNumbers) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.STARQUERY), str);
            }
            Log.d(TAG, "starQueryWaitFlag before waiting" + String.valueOf(starQueryWaitFlag));
            while (starQueryWaitFlag) ;
            Log.d(TAG, "starQueryWaitFlag after waiting" + String.valueOf(starQueryWaitFlag));
            matrixCursor = new MatrixCursor(new String[]{"key", "value"});
            for (String key : keyValueMap.keySet()) {
                if(!key.equals("dummyRecord")) {
                    matrixCursor.newRow().add(key).add(keyValueMap.get(key));
                }
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
                        Log.d(TAG,"Value of syncFlag in Server"+String.valueOf(syncFlag));
                        String modeString = strings.split("~")[0];
                        Log.d(TAG, "Mode String in Server-->" + modeString);
                        Mode mode = Mode.valueOf(modeString);
                        switch (mode) {
                            case HOLDERINSERTMODE:
                                while (syncFlag);
                                Log.d(TAG, "Inside HOLDERINSERTMODE in server");
                                String key=strings.split("~")[1];
                                String value=strings.split("~")[2];
                                insertRecord(key,value);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("holderMessageSaved");
                                ps.flush();
                                break;
                            case SUCCESSOR1MODE:
                                while (syncFlag);
                                Log.d(TAG, "Inside SUCCESSOR1MODE in server");
                                String key1=strings.split("~")[1];
                                String value1=strings.split("~")[2];
                                insertRecord(key1,value1);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("successor1MessageSaved");
                                ps.flush();
                                break;
                            case SUCCESSOR2MODE:
                                while (syncFlag);
                                Log.d(TAG, "Inside SUCCESSOR2MODE in server");
                                String key2=strings.split("~")[1];
                                String value2=strings.split("~")[2];
                                insertRecord(key2,value2);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("successor2MessageSaved");
                                ps.flush();
                                break;
                            case QUERY:
                                while (syncFlag);
                                Log.d(TAG, "Inside QUERYMODE in server");
                                String selectionQuery=strings.split("~")[1];
                                String fetchedQuery=queryRecord(selectionQuery);
                                Log.d(TAG,"fetchedQuery in QueryMode in Server"+fetchedQuery);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println(fetchedQuery);
                                ps.flush();
                                break;
                            case REDIRECTQUERY:
                                while (syncFlag);
                                Log.d(TAG, "Inside REDIRECTQUERY in server");
                                String redirectedQuery=strings.split("~")[1];
                                String fetchedRedirectedQuery=queryRecord(redirectedQuery);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println(fetchedRedirectedQuery);
                                ps.flush();
                                break;
                            case STARQUERY:
                                Log.d(TAG, "Inside STARQUERYMODE in server");
                                String queryString=getQueryString();
                                Log.d(TAG, "QueryString is->"+queryString);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println(queryString);
                                ps.flush();
                                break;
                            case DELETE:
                                Log.d(TAG, "Inside DELETEMODE in server");
                                String selectionQueryDelete=strings.split("~")[1];
                                deleteRecord(selectionQueryDelete);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("recordDeleted");
                                ps.flush();
                                break;
                            case DELETESTAR:
                                Log.d(TAG, "Inside DELETESTARMODE in server");
                                deleteAll();
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println("DeletedAll");
                                ps.flush();
                                break;
                            case SYNCSUCCESSORS:
                                Log.d(TAG, "Inside SYNCSUCCESSORS in server");
                                String incomingPort=strings.split("~")[1];
                                String queryStringSuccessor=getQueryStringSuccessor(incomingPort);
                                Log.d(TAG, "QueryString in SYNCSUCCESSORS is->"+queryStringSuccessor);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println(queryStringSuccessor);
                                ps.flush();
                                break;
                            case SYNCPREDECESSORS:
                                Log.d(TAG, "Inside SYNCPREDECESSORS in server");
                                String incomingPortPredecessor=strings.split("~")[1];
                                String queryStringPredecessor=getQueryStringSuccessor(incomingPortPredecessor);
                                Log.d(TAG, "QueryString in SYNCSUCCESSORS is->"+queryStringPredecessor);
                                ps = new PrintStream
                                        (client.getOutputStream());
                                ps.println(queryStringPredecessor);
                                ps.flush();
                                break;
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
    public void redirectQuery(String firstSuccesor,String selectionQuery,String queryHolderPort){
        while (syncFlag);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.REDIRECTQUERY),firstSuccesor,selectionQuery,queryHolderPort);

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
                    Log.d(TAG, "HolderPort number in HOLDERINSERTMODE in client->"+holderPortNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(holderPortNumber));
                        //socket.setSoTimeout(1000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.HOLDERINSERTMODE)+"~"+key+"~"+value);
                            ps.flush();
                            //ps.close();
                            //check socket time out before closing the socket.
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String msg=in.readLine();
                            Log.d(TAG,"Message Received in holder mode from server-->"+msg);
                            if(msg!=null) {
                                if (msg.equals("holderMessageSaved")) {
                                    acknList = insertWaitMap.get(key + "$" + portStr);
                                    acknList.add("holderMessageSaved");
                                }
                            }
                            if(msg==null){
                                failureFlag=true;
                                failedNode=holderPort;
                            }
                            Log.d(TAG,"AckList is-->"+acknList.toString());
                        }
                    }catch (SocketTimeoutException e){
                        e.printStackTrace();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case SUCCESSOR1MODE:
                    Log.d(TAG, "Inside SUCCESSOR1MODE in client");
                    String successor1Port = msgs[1];
                    String key1 = msgs[2];
                    String  value1 = msgs[3];
                    String successor1PortNumber = String.valueOf((Integer.parseInt(successor1Port) * 2));
                    Log.d(TAG, "successor1PortNumber in client->"+successor1PortNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor1PortNumber));
                        //socket.setSoTimeout(1000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.SUCCESSOR1MODE)+"~"+key1+"~"+value1);
                            ps.flush();
                            // ps.close();
                            //check socket time out before closing the socket.
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String msg1=in.readLine();
                            Log.d(TAG,"Message Received in SUCCESSOR1MODE mode from server-->"+msg1);
                            if(msg1!=null) {
                                if (msg1.equals("successor1MessageSaved")) {
                                    acknList = insertWaitMap.get(key1 + "$" + portStr);
                                    acknList.add("successor1Done");
                                }
                            }
                            if(msg1==null){
                                Log.d(TAG,"Inside failure condition in holderMode");
                                failureFlag=true;
                                failedNode=successor1Port;
                            }
                            Log.d(TAG,"AckList is-->"+acknList.toString());
                        }
                    }catch (SocketTimeoutException e){
                        e.printStackTrace();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case SUCCESSOR2MODE:
                    Log.d(TAG, "Inside SUCCESSOR2MODE in client");
                    String successor2Port = msgs[1];
                    String key2 = msgs[2];
                    String value2 = msgs[3];
                    String successor2PortNumber = String.valueOf((Integer.parseInt(successor2Port) * 2));
                    Log.d(TAG, "successor1PortNumber in client->"+successor2PortNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor2PortNumber));
                        //socket.setSoTimeout(1000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.SUCCESSOR2MODE)+"~"+key2+"~"+value2);
                            ps.flush();
                            // ps.close();
                            //check socket time out before closing the socket.
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String msg2=in.readLine();
                            Log.d(TAG,"Message Received in SUCCESSOR2MODE mode from server-->"+msg2);
                            if(msg2!=null) {
                                if (msg2.equals("successor2MessageSaved")) {
                                    acknList = insertWaitMap.get(key2 + "$" + portStr);
                                    acknList.add("successor2Done");
                                }
                            }
                            if(msg2==null){
                                Log.d(TAG,"Inside failure condition in holderMode");
                                failureFlag=true;
                                failedNode=successor2Port;
                            }
                            Log.d(TAG,"AckList is-->"+acknList.toString());
                        }
                    }catch (SocketTimeoutException e){
                        e.printStackTrace();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                // new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.QUERY),queryHolderPort,selectionQuery);4
                case QUERY:
                    Log.d(TAG, "Inside QUERYMODE in client");
                    String queryHolderPort = msgs[1];
                    String selectionQuery = msgs[2];
                    String queryHolderPortNumber = String.valueOf((Integer.parseInt(queryHolderPort) * 2));
                    Log.d(TAG, "queryHolderPortNumber in client->"+queryHolderPortNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(queryHolderPortNumber));
                        //socket.setSoTimeout(1000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.QUERY)+"~"+selectionQuery);
                            ps.flush();
                            // queryHolderPort+"$"+selectionQuery
                            // ps.close();
                            //check socket time out before closing the socket.
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            finalFetchedQuery=in.readLine();
                            Log.d(TAG,"final query retrieved is"+finalFetchedQuery);
                            if(finalFetchedQuery!=null && finalFetchedQuery!="") {
                                waitMap.put(queryHolderPort + "$" + selectionQuery, false);
                            }
                            if(finalFetchedQuery==null){
                                Log.d(TAG,"Inside catch block in query mode");
                                failureFlag=true;
                                failedNode=queryHolderPort;
                                String[] Successors=getSuccessors(failedNode);
                                String successor=Successors[0];
                                redirectQuery(successor,selectionQuery,queryHolderPort);
                            }
                            Log.d(TAG,"Map condition in Query mode false for"+waitMap.get(queryHolderPort+"$"+selectionQuery));
                            //queryWaitFlag=false;
                        }
                    }catch (SocketTimeoutException e){
                    }

                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case REDIRECTQUERY:
                    Log.d(TAG, "Inside REDIRECTQUERYMODE in client");
                    String firstSuccessorPort = msgs[1];
                    String redirectedQuery=msgs[2];
                    String queryHolderPortRedirect=msgs[3];
                    String firstSuccessorPortNumber = String.valueOf((Integer.parseInt(firstSuccessorPort) * 2));
                    Log.d(TAG, "firstSuccessorPortNumber in client->"+firstSuccessorPortNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(firstSuccessorPortNumber));
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.REDIRECTQUERY)+"~"+redirectedQuery);
                            ps.flush();
                            // queryHolderPort+"$"+selectionQuery
                            // ps.close();
                            //check socket time out before closing the socket.
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            finalFetchedQuery=in.readLine();
                            Log.d(TAG,"final query retrieved in redirect mode is"+finalFetchedQuery);
                            waitMap.put(queryHolderPortRedirect+"$"+redirectedQuery,false);
                            Log.d(TAG,"Map condition in Redirected Mode false for"+queryHolderPortRedirect+"$"+redirectedQuery);
                            //queryWaitFlag=false;
                        }
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
                    break;
                case STARQUERY:
                    Log.d(TAG, "Inside STARQUERYMODE in client");
                    String port=msgs[1];
                    String portNumber = String.valueOf((Integer.parseInt(port) * 2));
                    Log.d(TAG, "queryStarPortNumber in client->"+portNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portNumber));
                        //socket.setSoTimeout(1000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.STARQUERY));
                            ps.flush();
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String queryString=in.readLine();
                            insertQueryStringIntoMap(queryString);
                            starQueryList.add("All keys added for"+port);
                            //In case of failure change the condition to greater than equal to 4.
                            //One can check the treeMapSize
                            if(!failureFlag) {
                                Log.d(TAG, "Inside STARQUERYMODE in client value of failureFlag false condition"+failureFlag);
                                if (starQueryList.size() == ringMap.size()) {
                                    starQueryWaitFlag = false;
                                }
                            }
                            if(failureFlag) {
                                Log.d(TAG, "Inside STARQUERYMODE in client value of failureFlag true condition"+failureFlag);
                                if (starQueryList.size() == ringMap.size()-1) {
                                    starQueryWaitFlag = false;
                                }
                            }
                        }
                    }
                    catch (SocketTimeoutException e){
                        Log.d(TAG,"Inside catch block in StarQuery mode");
                        failureFlag=true;
                        failedNode=port;
                        e.printStackTrace();
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
                    break;
                case DELETE:
                    Log.d(TAG, "Inside DELETEMODE in client");
                    String holderPortDelete=msgs[1];
                    String selectionQueryDelete = msgs[2];
                    String holderPortDeleteNumber = String.valueOf((Integer.parseInt(holderPortDelete) * 2));
                    Log.d(TAG, "holderPortDeleteNumber in client->"+holderPortDeleteNumber);
                    try{
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(holderPortDeleteNumber));
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.DELETE)+"~"+selectionQueryDelete);
                            ps.flush();
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String msg=in.readLine();
                            if(msg!=null) {
                                if (msg.equals("recordDeleted")) {
                                 /*   deleteList.add("record Deleted for" + holderPortDelete);
                                    if (deleteList.size() == 2) {
                                        deleteWaitFlag = false;
                                    }*/
                                    Log.d(TAG,"MEssageDeleted");
                                }
                            }
                            if(msg==null){
                                failureFlag=true;
                                failedNode=holderPortDelete;
                            }
                        }
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
                    break;
                case DELETESTAR:
                    Log.d(TAG, "Inside DELETESTARMODE in client");
                    String starholderPortDelete=msgs[1];
                    String starholderPortDeleteNumber = String.valueOf((Integer.parseInt(starholderPortDelete) * 2));
                    Log.d(TAG, "starholderPortDeleteNumber in client->"+starholderPortDeleteNumber);
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
                                deleteStarList.add("All keys deleted for"+starholderPortDelete);
                                if(!failureFlag) {
                                    Log.d(TAG, "Inside DELETESTARMODE in client the value of failure flag false condition"+failureFlag);
                                    if (deleteStarList.size() == ringMap.size()) {
                                        deleteWaitFlag = false;
                                    }
                                }
                                if(failureFlag) {
                                    Log.d(TAG, "Inside DELETESTARMODE in client the value of failure flag true condition"+failureFlag);
                                    if (deleteStarList.size() == ringMap.size()-1) {
                                        deleteWaitFlag = false;
                                    }
                                }

                            }
                        }
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
                    break;
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, String.valueOf(Mode.SYNCSUCCESSORS),str);
                case SYNCSUCCESSORS:
                    syncMapSuccessor=new HashMap<String,String>();
                    Log.d(TAG,"New hashmap object created in SYNCSUCCESSORS"+syncMapSuccessor.toString());
                    Log.d(TAG, "Inside SYNCSUCCESSORS in client");
                    String successorPort = msgs[1];
                    String successorPortNumber = String.valueOf((Integer.parseInt(successorPort) * 2));
                    Log.d(TAG, "successorPortNumber in client->"+successorPortNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPortNumber));
                        //socket.setSoTimeout(1000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.SYNCSUCCESSORS)+"~"+portStr);
                            ps.flush();
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String queryStringSuccessor=in.readLine();
                            insertQueryStringIntoSyncMap(queryStringSuccessor,syncMapSuccessor);
                            insertSyncMapIntoContentProvider(syncMapSuccessor);
                            syncList.add("successor done");
                            if(syncList.size()==3){
                                syncFlag=false;
                            }
                            Log.d(TAG,"syncList-->"+syncList.toString());
                            //queryWaitFlag=false;
                        }
                    }catch (SocketTimeoutException e){
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                case SYNCPREDECESSORS:
                    Log.d(TAG, "Inside SYNCPREDECESSORS in client");
                    syncMapPredecessor=new HashMap<String,String>();
                    Log.d(TAG,"New hashmap object created in SYNCPREDECESSORS"+syncMapPredecessor.toString());
                    String predecessorPort = msgs[1];
                    String predecessorPortNumber = String.valueOf((Integer.parseInt(predecessorPort) * 2));
                    Log.d(TAG, "successorPortNumber in client->"+predecessorPortNumber);
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(predecessorPortNumber));
                        //socket.setSoTimeout(1000);
                        if (socket.isConnected()) {
                            PrintStream ps = new PrintStream
                                    (socket.getOutputStream());
                            ps.println(String.valueOf(Mode.SYNCSUCCESSORS)+"~"+predecessorPort);
                            ps.flush();
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            String queryStringPredecessor=in.readLine();
                            insertQueryStringIntoSyncMap(queryStringPredecessor,syncMapPredecessor);
                            insertSyncMapIntoContentProvider(syncMapPredecessor);
                            syncList.add("predecessor done");
                            if(syncList.size()==3){
                                syncFlag=false;
                            }
                            Log.d(TAG,"syncList-->"+syncList.toString());
                            //queryWaitFlag=false;
                        }
                    }catch (SocketTimeoutException e){
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;

            }
            return null;
        }
    }
    public enum Mode {
        HOLDERINSERTMODE,SUCCESSOR1MODE,SUCCESSOR2MODE,QUERY,STARQUERY,DELETE,DELETESTAR,REDIRECTQUERY,SYNCSUCCESSORS,SYNCPREDECESSORS
    }
}