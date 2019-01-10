import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;


public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;
    private TableName table = TableName.valueOf("ScrabbleGames");
    private String primaryCf = "primaryCf";
    private String sideCf = "sideCf";

    // key sizing
    private final short key1Size = 4;
    private final short key2Size = 50;
    private final short key3Size = 4;
    private final short keyTotalSize = key1Size + key2Size + key3Size;

    //DEBUG
    private final boolean DEBUG = true;

    /**
     * The Constructor. Establishes the connection with HBase.
     *
     * @param zkHost Host address to connect to
     * @throws IOException
     */
    public HBaseScrabble(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    /**
     * Creates the table and splits it into region servers.
     *
     * @throws IOException
     */
    public void createTable() throws IOException {
        HTableDescriptor hTable = new HTableDescriptor(table);
        hTable.addFamily(new HColumnDescriptor(primaryCf).setMaxVersions(10));
        hTable.addFamily(new HColumnDescriptor(sideCf).setMaxVersions(10));
        hBaseAdmin.createTable(hTable);
        try {
            tableSplit(3);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Split table into region servers
     *
     * @param n_regions Number of region servers
     * @throws IOException
     * @throws InterruptedException
     */
    private void tableSplit(int n_regions) throws IOException, InterruptedException {
        int totalRows = 1542642;
        byte[] splitPoint;
        for (int i = 1; i < n_regions; i++) {
            splitPoint = generateStartKey(Integer.toString(totalRows / n_regions * i));
            hBaseAdmin.split(table.toBytes(), splitPoint);
            waitOnlineNewRegionsAfterSplit(splitPoint);
        }
        if (DEBUG)
            System.out.println("[INFO] " + table.toString() + " has been split into " + n_regions + " regions successfully");
    }

    /**
     * Checks the existence of the two regions divided by the startKey
     *
     * @param startKey Limit key between the regions to check
     * @throws IOException
     * @throws InterruptedException
     */
    private void waitOnlineNewRegionsAfterSplit(byte[] startKey) throws IOException, InterruptedException {
        short sleepTime = 500;
        short maxRetries = 5;
        HRegionInfo newLeftSideRegion = null;
        HRegionInfo newRightSideRegion = null;

        int retry = 1;
        do {

            List<HRegionInfo> regions = hBaseAdmin.getTableRegions(table);
            Iterator<HRegionInfo> iter = regions.iterator();

            while (iter.hasNext() && (newLeftSideRegion == null || newRightSideRegion == null)) {
                HRegionInfo regInfo = iter.next();
                if (Arrays.equals(regInfo.getEndKey(), startKey)) {
                    newLeftSideRegion = regInfo;
                }
                if (Arrays.equals(regInfo.getStartKey(), startKey)) {
                    newRightSideRegion = regInfo;
                }
            }
            if (newLeftSideRegion == null || newRightSideRegion == null) {
                if (DEBUG) System.out.print("Waiting " + sleepTime + " ms...");
                Thread.sleep(sleepTime);
                retry++;
            }
        } while ((newLeftSideRegion == null || newRightSideRegion == null) && retry <= maxRetries);

        if (retry > maxRetries) {
            throw new IOException("Split failed, can't find regions with startKey and endKey = " + Bytes.toStringBinary(startKey));
        }
    }

    /**
     * Loads the data from a directory into the hBase table
     *
     * @param folder Path of the directory from which the source data is fetched
     * @throws IOException
     */
    public void loadTable(String folder) throws IOException {

        HTable hTable = new HTable(config, table);

        // TODO: How to deal with the files? Atm we're using the first file in the directory

        File folder_file = new File(folder);
        File[] listOfFiles = folder_file.listFiles();

        if (listOfFiles != null) {
            if (DEBUG) System.out.println("[INFO] Reading " + listOfFiles[0].getName());
            BufferedReader csvReader = new BufferedReader(new FileReader(listOfFiles[0]));
            String[] header = csvReader.readLine().split(","); // skip first line
            String line;
            String[] nextRecord;
            int c = 0;
            ArrayList<Put> putList = new ArrayList<>();
            while ((line = csvReader.readLine()) != null) {
                nextRecord = line.split(",");
                putList.add(putAll(hTable, nextRecord[1], nextRecord[4], nextRecord[9], nextRecord[3], nextRecord[0], nextRecord[2],
                        nextRecord[5], nextRecord[6], nextRecord[7], nextRecord[8],
                        nextRecord[10], nextRecord[11], nextRecord[12], nextRecord[13], nextRecord[14],
                        nextRecord[15], nextRecord[16], nextRecord[17], nextRecord[18]));

                if (DEBUG && c % 10000 == 0) {
                    System.out.println("##### Inserted line " + c + ", " + (c / 1542642 * 100) + "% done.");
                    /*
                    for (int i = 0; i < nextRecord.length; i++) {
                        System.out.println(header[i] + " : " + nextRecord[i]);
                    }
                    */
                }
                c++;
                if (putList.size() == 10000) {
                    hTable.put(putList);
                    putList = new ArrayList<>();
                }
            }
            hTable.put(putList);

            if (DEBUG) System.out.println("[INFO] Inserted " + c + " lines.");
        } else {
            System.err.println("[ERROR] There are no files in the directory.");
        }
    }

    /**
     * Creates the put object and inserts the single row into the hBase table
     *
     * @param hTable          Reference table
     * @param tourneyid
     * @param winnername
     * @param loserid
     * @param winnerid
     * @param gameid
     * @param tie
     * @param winnerscore
     * @param winneroldrating
     * @param winnernewrating
     * @param winnerpos
     * @param losername
     * @param loserscore
     * @param loseroldrating
     * @param losernewrating
     * @param loserpos
     * @param round
     * @param division
     * @param date
     * @param lexicon
     * @throws InterruptedIOException
     * @throws RetriesExhaustedWithDetailsException
     */
    private Put putAll(HTable hTable, String tourneyid, String winnername, String loserid, String winnerid, String gameid, String tie,
                       String winnerscore, String winneroldrating, String winnernewrating, String winnerpos,
                       String losername, String loserscore, String loseroldrating, String losernewrating, String loserpos,
                       String round, String division, String date, String lexicon) throws InterruptedIOException, RetriesExhaustedWithDetailsException {

        // primary column family
        byte[] key = createKey(tourneyid, winnername, gameid);
        byte[] byte_tourneyid = tourneyid.getBytes();
        byte[] byte_winnername = winnername.getBytes();
        byte[] byte_loserid = loserid.getBytes();
        byte[] byte_winnerid = winnerid.getBytes();
        byte[] byte_gameid = gameid.getBytes();
        byte[] byte_tie = tie.getBytes();

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
        byte[] byte_lexicon = lexicon.getBytes();

        // Put command
        Put put = new Put(createKey(tourneyid, winnername, gameid));
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

        return put;
    }

    /**
     * Creates the key for a specific entry
     * <p>
     * 4 byte for the tourneyid (max in the dataset: 3021) = 32 bit (in Two's Complement)
     * 50 byte for the winnername
     * 4 byte per il gameid (max in the dataset: 1823783) = 32 bit (in Two's Complement)
     *
     * @param tourneyid  Tourney ID
     * @param winnername Winner name
     * @param gameid     Game ID
     * @return Array of bytes containing the generated key
     */
    private byte[] createKey(String tourneyid, String winnername, String gameid) {
        byte[] key = new byte[keyTotalSize];
        byte[] tourneyid_bin = ByteBuffer.allocate(key1Size).putInt(Integer.valueOf(tourneyid)).array();
        byte[] gameid_bin = ByteBuffer.allocate(key3Size).putInt(Integer.valueOf(gameid)).array();
        System.arraycopy(tourneyid_bin, 0, key, 0, tourneyid_bin.length);
        System.arraycopy(winnername.getBytes(), 0, key, key1Size, winnername.length());
        System.arraycopy(gameid_bin, 0, key, key1Size + key2Size, gameid_bin.length);
        return key;
    }

    /**
     * Creates the lowermost key of the specific tourney ID
     *
     * @param tourneyid
     * @return An array of bytes containing the first key for the tourney ID
     */
    private byte[] generateStartKey(String tourneyid) {
        byte[] key = new byte[keyTotalSize];
        byte[] tourneyid_bin = ByteBuffer.allocate(key1Size).putInt(Integer.valueOf(tourneyid)).array();
        System.arraycopy(tourneyid_bin, 0, key, 0, tourneyid_bin.length);
        for (int i = key1Size; i < keyTotalSize; i++) {
            key[i] = (byte) 0; // not -128 because otherwise the key starts with 1s (because of the Two's Complement)
        }
        return key;
    }

    /**
     * Creates the uppermost key of the specific tourney ID
     *
     * @param tourneyid
     * @return An array of bytes containing the last key for the tourney ID
     */
    private byte[] generateEndKey(String tourneyid) {
        byte[] key = new byte[keyTotalSize];
        byte[] tourneyid_bin = ByteBuffer.allocate(key1Size).putInt(Integer.valueOf(tourneyid)).array();
        System.arraycopy(tourneyid_bin, 0, key, 0, tourneyid_bin.length);
        for (int i = key1Size; i < keyTotalSize; i++) {
            key[i] = (byte) 127;
        }
        return key;
    }

    /**
     * Creates the lowermost key of the specific tourney ID and winner name
     *
     * @param tourneyid
     * @param winnername
     * @return An array of bytes containing the first key for the tourney ID and winner name
     */
    private byte[] generateStartKey(String tourneyid, String winnername) {
        byte[] key = new byte[keyTotalSize];
        byte[] tourneyid_bin = ByteBuffer.allocate(key1Size).putInt(Integer.valueOf(tourneyid)).array();
        System.arraycopy(tourneyid_bin, 0, key, 0, tourneyid_bin.length);
        System.arraycopy(winnername.getBytes(), 0, key, key1Size, winnername.length());
        for (int i = key2Size; i < keyTotalSize; i++) {
            key[i] = (byte) 0; // not -128 because otherwise the key starts with 1s (because of the Two's Complement)
        }
        System.out.println();
        return key;
    }

    /**
     * Creates the uppermost key of the specific tourney ID and winner name
     *
     * @param tourneyid
     * @param winnername
     * @return An array of bytes containing the last key for the tourney ID and winner name
     */
    private byte[] generateEndKey(String tourneyid, String winnername) {
        byte[] key = new byte[keyTotalSize];
        byte[] tourneyid_bin = ByteBuffer.allocate(key1Size).putInt(Integer.valueOf(tourneyid)).array();
        System.arraycopy(tourneyid_bin, 0, key, 0, tourneyid_bin.length);
        System.arraycopy(winnername.getBytes(), 0, key, key1Size, winnername.length());
        for (int i = key2Size; i < keyTotalSize; i++) {
            key[i] = (byte) 127;
        }
        return key;
    }

    /**
     * Returns all the opponents (Loserid) of a given Winnername in a tournament (Tourneyid).
     *
     * @param tourneyid
     * @param winnername
     * @return Array of results of the query
     * @throws IOException
     */
    public List<String> query1(String tourneyid, String winnername) throws IOException {
        HTable hTable = new HTable(config, table);

        byte[] startKey = generateStartKey(tourneyid, winnername);
        byte[] endKey = generateEndKey(tourneyid, winnername);
        List<String> res = new ArrayList<>();

        Scan scan = new Scan(startKey, endKey);
        ResultScanner rs = hTable.getScanner(scan);

        Result result = rs.next();
        while (result != null && !result.isEmpty()) {
            res.add(Bytes.toString(result.getValue(primaryCf.getBytes(), "loserid".getBytes())));
            if (DEBUG)
                System.out.println("Loserid : " + Bytes.toString(result.getValue(primaryCf.getBytes(), "loserid".getBytes())));
            result = rs.next();
        }

        return res;
    }

    /**
     * Returns the ids of the players (winner and loser) that have participated more than once
     * in all tournaments between two given Tourneyids.
     *
     * @param firsttourneyid
     * @param lasttourneyid
     * @return Array of results of the query
     * @throws IOException
     */
    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {
        HTable hTable = new HTable(config, table);

        byte[] startKey = generateStartKey(firsttourneyid);
        byte[] endKey = generateEndKey(lasttourneyid);

        Scan scan = new Scan(startKey, endKey);
        ResultScanner rs = hTable.getScanner(scan);
        HashSet<String> freq = new HashSet<>();
        HashSet<String> res = new HashSet<>();

        Result result = rs.next();
        while (result != null && !result.isEmpty()) {
            String winnerid = Bytes.toString(result.getValue(primaryCf.getBytes(), "loserid".getBytes()));
            String loserid = Bytes.toString(result.getValue(primaryCf.getBytes(), "winnerid".getBytes()));
            if (!freq.add(winnerid))
                res.add(winnerid);
            if (!freq.add(loserid))
                res.add(loserid);
            result = rs.next();
        }
        return new ArrayList<>(res);
    }

    /**
     * Given a Tourneyid, the query returns the Gameid, the ids of the two participants that
     * have finished in tie.
     *
     * @param tourneyid
     * @return Array of results of the query
     * @throws IOException
     */
    public List<String> query3(String tourneyid) throws IOException {
        HTable hTable = new HTable(config, table);

        byte[] startKey = generateStartKey(tourneyid);
        byte[] endKey = generateEndKey(tourneyid);
        List<String> res = new ArrayList<>();

        Scan scan = new Scan(startKey, endKey);
        SingleColumnValueFilter tieFilter = new SingleColumnValueFilter(
                primaryCf.getBytes(),
                "tie".getBytes(),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("True")
        );
        scan.setFilter(tieFilter);
        ResultScanner rs = hTable.getScanner(scan);
        Result result = rs.next();
        while (result != null && !result.isEmpty()) {
            String gId = Bytes.toString(result.getValue(primaryCf.getBytes(), "gameid".getBytes()));
            String wId = Bytes.toString(result.getValue(primaryCf.getBytes(), "winnerid".getBytes()));
            String lId = Bytes.toString(result.getValue(primaryCf.getBytes(), "loserid".getBytes()));
            res.add(gId + "-" + wId + "-" + lId);
            result = rs.next();
        }

        return res;
    }


    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if (args[1].toUpperCase().equals("CREATETABLE")) {
            hBaseScrabble.createTable();
        } else if (args[1].toUpperCase().equals("LOADTABLE")) {
            if (args.length != 3) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            } else if (!(new File(args[2])).isDirectory()) {
                System.out.println("Error: Folder " + args[2] + " does not exist.");
                System.exit(-2);
            }
            hBaseScrabble.loadTable(args[2]);
        } else if (args[1].toUpperCase().equals("QUERY1")) {
            if (args.length != 4) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }

            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            System.out.println("There are " + opponentsName.size() + " opponents of winner " + args[3] + " that play in tourney " + args[2] + ".");
            System.out.println("The list of opponents is: " + Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
        } else if (args[1].toUpperCase().equals("QUERY2")) {
            if (args.length != 4) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            List<String> playerNames = hBaseScrabble.query2(args[2], args[3]);
            System.out.println("There are " + playerNames.size() + " players that participates in more than one tourney between tourneyid " + args[2] + " and tourneyid " + args[3] + " .");
            System.out.println("The list of players is: " + Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
        } else if (args[1].toUpperCase().equals("QUERY3")) {
            if (args.length != 3) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            List<String> games = hBaseScrabble.query3(args[2]);
            System.out.println("There are " + games.size() + " that ends in tie in tourneyid " + args[2] + " .");
            System.out.println("The list of games is: " + Arrays.toString(games.toArray(new String[games.size()])));
        } else {
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }
}