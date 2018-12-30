import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Arrays;
import java.util.List;



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

        HTable hTable = new HTable(config,table);

        // TODO: input as a file or a folder? ATM is a file.

        // Alternative (folder case):

        File folder_file = new File(folder);
        File[] listOfFiles = folder_file.listFiles();

        if (listOfFiles != null) {
            System.out.println("[INFO] Reading " + listOfFiles[0].getName());
            BufferedReader csvReader = new BufferedReader(new FileReader(listOfFiles[0]));
            String[] header = csvReader.readLine().split(","); // skip first line
            String line;
            String[] nextRecord;
            int c = 0;
            while ((line = csvReader.readLine()) != null) {
                nextRecord = line.split(",");
                putAll(hTable, nextRecord[1], nextRecord[4], nextRecord[9], nextRecord[3], nextRecord[0], nextRecord[2],
                        nextRecord[5],nextRecord[6],nextRecord[7],nextRecord[8],
                        nextRecord[10],nextRecord[11],nextRecord[12],nextRecord[13], nextRecord[14],
                        nextRecord[15],nextRecord[16],nextRecord[17],nextRecord[18]);

                if (c%100 == 0) {
                    System.out.println("##### Inserted line " + c);
                    for (int i = 0; i < nextRecord.length; i++) {
                        System.out.println(header[i] + " : " + nextRecord[i]);
                    }
                }
                c++;
            }
            System.out.println("[INFO] Inserted " + c + " lines.");
        }
        else {
            System.out.println("[INFO] There are no files in the directory.");
        }
    }

    private void putAll(HTable hTable, String tourneyid, String winnername, String loserid, String winnerid, String gameid, String tie,
                        String winnerscore, String winneroldrating, String winnernewrating, String winnerpos,
                        String losername, String loserscore, String loseroldrating, String losernewrating, String loserpos,
                        String round, String division, String date, String lexicon) throws InterruptedIOException, RetriesExhaustedWithDetailsException {

        // primary column family
        byte[] key = Bytes.toBytes(tourneyid + winnername);
        byte[] byte_tourneyid = tourneyid.getBytes();
        byte[] byte_winnername = winnername.getBytes();
        byte[] byte_loserid = loserid.getBytes();
        byte[] byte_winnerid = winnerid.getBytes();
        byte[] byte_gameid = gameid.getBytes();
        byte[] byte_tie = tie.getBytes(); //Bytes.toBytes(tie.equals("True")? 1:0);

        // side column family
        byte[] byte_winnerscore = winnerscore.getBytes();
        byte[] byte_winneroldrating = winneroldrating.getBytes();
        byte[] byte_winnernewrating = winnernewrating.getBytes();
        byte[] byte_winnerpos = winnerpos.getBytes();
        byte[] byte_losername = losername.getBytes();
        byte[] byte_loserscore = loserscore.getBytes();
        byte[] byte_loseroldrating = loseroldrating.getBytes();
        byte[] byte_losernewrating = losernewrating.getBytes();
        byte[] byte_loserpos = loserpos.getBytes();
        byte[] byte_round = round.getBytes();
        byte[] byte_division = division.getBytes();
        byte[] byte_date = date.getBytes();
        byte[] byte_lexicon = lexicon.getBytes(); //Bytes.toBytes(lexicon.equals("True")? 1:0);

        // Put command
        Put put = new Put(key);
        long ts = System.currentTimeMillis();

        put.add(primaryCf.getBytes(), "tourneyid".getBytes(), ts, byte_tourneyid);
        put.add(primaryCf.getBytes(), "winnername".getBytes(), ts, byte_winnername);
        put.add(primaryCf.getBytes(), "loserid".getBytes(), ts, byte_loserid);
        put.add(primaryCf.getBytes(), "winnerid".getBytes(), ts, byte_winnerid);
        put.add(primaryCf.getBytes(), "gameid".getBytes(), ts, byte_gameid);
        put.add(primaryCf.getBytes(), "tie".getBytes(), ts, byte_tie);

        put.add(sideCf.getBytes(), "winnerscore".getBytes(), ts, byte_winnerscore);
        put.add(sideCf.getBytes(), "winneroldrating".getBytes(), ts, byte_winneroldrating);
        put.add(sideCf.getBytes(), "winnernewrating".getBytes(), ts, byte_winnernewrating);
        put.add(sideCf.getBytes(), "winnerpos".getBytes(), ts, byte_winnerpos);
        put.add(sideCf.getBytes(), "losername".getBytes(), ts, byte_losername);
        put.add(sideCf.getBytes(), "loserscore".getBytes(), ts, byte_loserscore);
        put.add(sideCf.getBytes(), "loseroldrating".getBytes(), ts, byte_loseroldrating);
        put.add(sideCf.getBytes(), "losernewrating".getBytes(), ts, byte_losernewrating);
        put.add(sideCf.getBytes(), "loserpos".getBytes(), ts, byte_loserpos);
        put.add(sideCf.getBytes(), "round".getBytes(), ts, byte_round);
        put.add(sideCf.getBytes(), "division".getBytes(), ts, byte_division);
        put.add(sideCf.getBytes(), "date".getBytes(), ts, byte_date);
        put.add(sideCf.getBytes(), "lexicon".getBytes(), ts, byte_lexicon);

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
