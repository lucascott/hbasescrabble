import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;
    TableName table = TableName.valueOf("ScrabbleGames");
    String primaryCf = "primaryCf";
    String sideCf = "sideCf";

    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseScrabble(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    public void createTable() throws IOException {

        HTableDescriptor hTable = new HTableDescriptor(table);
        hTable.addFamily(new HColumnDescriptor(primaryCf).setMaxVersions(10));
        hTable.addFamily(new HColumnDescriptor(sideCf).setMaxVersions(10));
        this.hBaseAdmin.createTable(hTable);
    }

    public void loadTable(String folder)throws IOException{
        // TODO: input as a file or a folder? ATM is a file.
        CSVReader csvReader = new CSVReader(new BufferedReader(new FileReader(new File(folder))));
        String[] nextRecord;
        int count = 0;
        while ((nextRecord = csvReader.readNext()) != null && count < 10) {
            count++;
            System.out.println(nextRecord);
        }
    }

    private void putAll(HTable hTable, int tourneyid, String winnername, int loserid, int winnerid, int gameid, String tie,
                        int winnerscore, int winneroldrating, int winnernewrating, int winnerpos, int loseroldrating,
                        int losernewrating, int loserpos, int round, int division, String date, String lexicon) throws InterruptedIOException, RetriesExhaustedWithDetailsException {

        // primary column family
        boolean bool_tie = tie.equals("True");
        byte[] key = Bytes.toBytes(tourneyid + winnername);
        byte[] byte_tourneyid = Bytes.toBytes(tourneyid);
        byte[] byte_winnername = Bytes.toBytes(winnername);
        byte[] byte_loserid = Bytes.toBytes(loserid);
        byte[] byte_winnerid = Bytes.toBytes(winnerid);
        byte[] byte_gameid = Bytes.toBytes(gameid);
        byte[] byte_bool_tie = Bytes.toBytes(bool_tie);

        // side column family
        boolean bool_lexicon = lexicon.equals("True");
        byte[] byte_winnerscore = Bytes.toBytes(winnerscore);
        byte[] byte_winneroldrating = Bytes.toBytes(winneroldrating);
        byte[] byte_winnernewrating = Bytes.toBytes(winnernewrating);
        byte[] byte_winnerpos = Bytes.toBytes(winnerpos);
        byte[] byte_loseroldrating = Bytes.toBytes(loseroldrating);
        byte[] byte_losernewrating = Bytes.toBytes(losernewrating);
        byte[] byte_loserpos = Bytes.toBytes(loserpos);
        byte[] byte_round = Bytes.toBytes(round);
        byte[] byte_division = Bytes.toBytes(division);
        byte[] byte_date = Bytes.toBytes(date);
        byte[] byte_bool_lexicon = Bytes.toBytes(bool_lexicon);

        // Put command
        Put put = new Put(key);
        long ts = System.currentTimeMillis();

        put.add(primaryCf.getBytes(), "tourneyid".getBytes(), ts, byte_tourneyid);
        put.add(primaryCf.getBytes(), "winnername".getBytes(), ts, byte_winnername);
        put.add(primaryCf.getBytes(), "loserid".getBytes(), ts, byte_loserid);
        put.add(primaryCf.getBytes(), "winnerid".getBytes(), ts, byte_winnerid);
        put.add(primaryCf.getBytes(), "gameid".getBytes(), ts, byte_gameid);
        put.add(primaryCf.getBytes(), "bool_tie".getBytes(), ts, byte_bool_tie);

        put.add(sideCf.getBytes(), "winnerscore".getBytes(), ts, byte_winnerscore);
        put.add(sideCf.getBytes(), "winneroldrating".getBytes(), ts, byte_winneroldrating);
        put.add(sideCf.getBytes(), "winnernewrating".getBytes(), ts, byte_winnernewrating);
        put.add(sideCf.getBytes(), "winnerpos".getBytes(), ts, byte_winnerpos);
        put.add(sideCf.getBytes(), "loseroldrating".getBytes(), ts, byte_loseroldrating);
        put.add(sideCf.getBytes(), "losernewrating".getBytes(), ts, byte_losernewrating);
        put.add(sideCf.getBytes(), "loserpos".getBytes(), ts, byte_loserpos);
        put.add(sideCf.getBytes(), "round".getBytes(), ts, byte_round);
        put.add(sideCf.getBytes(), "division".getBytes(), ts, byte_division);
        put.add(sideCf.getBytes(), "date".getBytes(), ts, byte_date);
        put.add(sideCf.getBytes(), "bool_lexicon".getBytes(), ts, byte_bool_lexicon);

        hTable.put(put);
    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += values[keyId];
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }



    public List<String> query1(String tourneyid, String winnername) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;

    }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }

    public List<String> query3(String tourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }


    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLE")){
            hBaseScrabble.createTable();
        }
        else if(args[1].toUpperCase().equals("LOADTABLE")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseScrabble.loadTable(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }

            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            System.out.println("There are "+opponentsName.size()+" opponents of winner "+args[3]+" that play in tourney "+args[2]+".");
            System.out.println("The list of opponents is: "+Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            List<String> playerNames =hBaseScrabble.query2(args[2], args[3]);
            System.out.println("There are "+playerNames.size()+" players that participates in more than one tourney between tourneyid "+args[2]+" and tourneyid "+args[3]+" .");
            System.out.println("The list of players is: "+Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            List<String> games = hBaseScrabble.query3(args[2]);
            System.out.println("There are "+games.size()+" that ends in tie in tourneyid "+args[2]+" .");
            System.out.println("The list of games is: "+Arrays.toString(games.toArray(new String[games.size()])));
        }
        else{
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }



}
