import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;


/**
 * @author Ainhoa Azqueta (aazqueta@fi.upm.es)
 * @date 2/12/17.
 */
public class HBaseExample {

    private Configuration config;
    private HBaseAdmin hBaseAdmin;
    private byte[] table = Bytes.toBytes("Users");

    public HBaseExample(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }


    private void createTable() throws IOException {
        byte[] cf = Bytes.toBytes("BasicData");

        HTableDescriptor hTable = new HTableDescriptor(table);
        HColumnDescriptor family = new HColumnDescriptor(cf);
        family.setMaxVersions(10); // Default is 3.
        hTable.addFamily(family);

        this.hBaseAdmin.createTable(hTable);
    }

    private void deleteTable() throws IOException {
        this.hBaseAdmin.disableTable(table);
        this.hBaseAdmin.deleteTable(table);
    }

    private void put() throws IOException {
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column1 = Bytes.toBytes("PROVINCE");
        byte[] column2 = Bytes.toBytes("LAST_LOGIN");

        HTable hTable = new HTable(config,table);

        putUser(hTable,cf,column1,column2,"JUAN","ZARAGOZA","12/2/17");
        putUser(hTable,cf,column1,column2,"MARIA","MADRID","10/28/17");
        putUser(hTable,cf,column1,column2,"PEPE","SEVILLA","11/30/17");
        putUser(hTable,cf,column1,column2,"MARTA","PONTEVEDRA","12/1/17");
        putUser(hTable,cf,column1,column2,"JESUS","ALICANTE","9/17/17");
        putUser(hTable,cf,column1,column2,"ANTONIO","JAEN","12/17/17");
        putUser(hTable,cf,column1,column2,"LUCAS","LUGO","11/28/17");
        putUser(hTable,cf,column1,column2,"MARTINA","TERUEL","11/10/17");
        putUser(hTable,cf,column1,column2,"LUCIA","BARCELONA","12/4/17");
        putUser(hTable,cf,column1,column2,"TERESA","MADRID","12/17/17");
        putUser(hTable,cf,column1,column2,"IRATI","NAVARRA","12/2/17");
        putUser(hTable,cf,column1,column2,"JOAN","GIRONA","11/28/17");
        putUser(hTable,cf,column1,column2,"ALEJANDRO","ZARAGOZA","11/30/17");
        putUser(hTable,cf,column1,column2,"ANDRES","BARCELONA","11/1/17");
        putUser(hTable,cf,column1,column2,"FERNANDO","MADRID","9/17/17");
    }

    private void putUser(HTable hTable, byte[] cf, byte[] column1, byte[] column2, String user, String province, String lastLogin) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        byte[] key = Bytes.toBytes(user);
        byte[] valueProvince = Bytes.toBytes(province);
        byte[] valueLastLogin = Bytes.toBytes(lastLogin);

        Put put = new Put(key);
        long ts = System.currentTimeMillis();
        put.add(cf, column1, ts, valueProvince);
        put.add(cf, column2, ts, valueLastLogin);

        hTable.put(put);

        System.out.println("Added user: "+user);
    }

    private void delete() throws IOException {
        HTable hTable = new HTable(config,table);

        byte[] key = Bytes.toBytes("JUAN");

        Delete delete = new Delete(key);
        hTable.delete(delete);

        System.out.println("Deleted user: JUAN");
    }

    private void get() throws IOException {
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("PROVINCE");
        HTable hTable = new HTable(config,table);

        byte[] key = Bytes.toBytes("JUAN");

        Get get = new Get(key);
        Result result = hTable.get(get);

        String province = Bytes.toString(result.getValue(cf, column));
        System.out.println("User JUAN - Province: "+province);
    }

    private void getNVersionRow() throws IOException {
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column1 = Bytes.toBytes("PROVINCE");
        byte[] column2 = Bytes.toBytes("LAST_LOGIN");

        HTable hTable = new HTable(config,table);

        System.out.println("Modifying system access for user JUAN...");
        putUser(hTable,cf,column1,column2,"JUAN","SORIA","12/8/17");
        putUser(hTable,cf,column1,column2,"JUAN","BARCELONA","12/9/17");
        putUser(hTable,cf,column1,column2,"JUAN","BARCELONA","12/10/17");
        putUser(hTable,cf,column1,column2,"JUAN","GIRONA","12/11/17");
        putUser(hTable,cf,column1,column2,"JUAN","GIRONA","12/12/17");
        putUser(hTable,cf,column1,column2,"JUAN","ZARAGOZA","12/13/17");

        byte[] key = Bytes.toBytes("JUAN");

        Get get = new Get(key);
        get.setMaxVersions(5);
        Result result = hTable.get(get);

        List<Cell> valuesColumn1 = result.getColumnCells(cf,column1);
        List<Cell> valuesColumn2 = result.getColumnCells(cf,column2);

        System.out.println("Obtaining all system access for user JUAN...");
        for ( int i = 0; i < valuesColumn1.size(); i++){
            String province = Bytes.toString(CellUtil.cloneValue(valuesColumn1.get(i)));
            String lastLogin = Bytes.toString(CellUtil.cloneValue(valuesColumn2.get(i)));
            System.out.println( "User JUAN - Province: "+province+ " Last_Login: " +lastLogin);
        }
    }

    private void getSpecificColumn() throws IOException {
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("LAST_LOGIN");
        HTable hTable = new HTable(config,table);

        byte[] key = Bytes.toBytes("JUAN");

        Get get = new Get(key);
        get.addColumn(cf,column);
        Result result = hTable.get(get);

        String lastLogin = Bytes.toString(result.getValue(cf, column));
        System.out.println("User JUAN - Last_Login: "+lastLogin);
    }


    private void scan() throws IOException {
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("PROVINCE");
        HTable hTable = new HTable(config,table);

        Scan scan = new Scan();
        ResultScanner rs = hTable.getScanner(scan);

        Result result = rs.next();
        while (result!=null && !result.isEmpty()){
            String key = Bytes.toString(result.getRow());
            String province = Bytes.toString(result.getValue(cf,column));
            System.out.println("Key: "+key+" Province: "+province);
            result = rs.next();
        }
    }

    private void rangeScan() throws IOException {
        byte[] cf = Bytes.toBytes("BasicData");
        byte[] column = Bytes.toBytes("PROVINCE");
        HTable hTable = new HTable(config,table);

        byte[] startKey = Bytes.toBytes("JUAN");
        byte[] endKey = Bytes.toBytes("PEPE");

        Scan scan = new Scan(startKey,endKey);
        ResultScanner rs = hTable.getScanner(scan);

        Result result = rs.next();
        while (result!=null && !result.isEmpty()){
            String key = Bytes.toString(result.getRow());
            String province = Bytes.toString(result.getValue(cf,column));
            System.out.println("Key: "+key+" Province: "+province);
            result = rs.next();
        }
    }

    private void split() throws IOException, InterruptedException {
        byte[] splitPoint1 = Bytes.toBytes("IRATI");
        byte[] splitPoint2 = Bytes.toBytes("MARIA");

        hBaseAdmin.split(table, splitPoint1);
        waitOnlineNewRegionsAfterSplit(splitPoint1);
        hBaseAdmin.split(table, splitPoint2);
        waitOnlineNewRegionsAfterSplit(splitPoint2);
    }

    private void waitOnlineNewRegionsAfterSplit(byte[] startKey) throws IOException, InterruptedException {

        HRegionInfo newLeftSideRegion = null;
        HRegionInfo newRightSideRegion = null;

        int retry = 1;
        do {
            Thread.sleep(1000);
            System.out.print("Sleeping");

            List<HRegionInfo> regions = hBaseAdmin.getTableRegions(table);
            Iterator<HRegionInfo> iter = regions.iterator();

            while (iter.hasNext() && (newLeftSideRegion == null || newRightSideRegion == null)) {
                HRegionInfo rinfo = iter.next();
                if (Arrays.equals(rinfo.getEndKey(), startKey)) {
                    newLeftSideRegion = rinfo;
                }
                if (Arrays.equals(rinfo.getStartKey(), startKey)) {
                    newRightSideRegion = rinfo;
                }
            }
            retry++;
        } while (newLeftSideRegion == null && newRightSideRegion == null && retry <= 50);

        if (retry > 3){
            throw new IOException("split failed, can't find regions with startKey and endKey = "+Bytes.toStringBinary(startKey));
        }
    }

    private void move() throws IOException, InterruptedException {
        ServerName[] servers = getServers();

        int i = 0;
        for(HRegionInfo hRegionInfo : hBaseAdmin.getTableRegions(table)){
            String originalServer = getServerOfRegion(hRegionInfo).getHostAndPort();
            ServerName finalServer = servers[i];

            if(originalServer.compareTo(finalServer.getHostAndPort())!=0){
                byte[] snb = (finalServer.getHostname() + "," + finalServer.getPort() + "," + finalServer.getStartcode()).getBytes();
                hBaseAdmin.move(hRegionInfo.getEncodedNameAsBytes(), snb);
                wait(10);
            }
            i++;
            if (i == servers.length)
                i = 0;

        }
    }

    private ServerName[] getServers() throws IOException {
        Collection<ServerName> serverNames = hBaseAdmin.getClusterStatus().getServers();
        return serverNames.toArray(new ServerName[serverNames.size()]);
    }

    private ServerName getServerOfRegion(HRegionInfo hri) throws IOException {
        for (ServerName sm : getServers()){
            for(HRegionInfo hRegionInfo : hBaseAdmin.getOnlineRegions(sm)){
                if(Bytes.compareTo(hri.getEncodedNameAsBytes(),hRegionInfo.getEncodedNameAsBytes())==0){
                    return sm;
                }
            }
        }
        return null;
    }

    private void merge() throws IOException, InterruptedException {
        List<HRegionInfo> tableHRs = null;
        while ((tableHRs = hBaseAdmin.getTableRegions(table)).size()!=1){
            hBaseAdmin.mergeRegions(tableHRs.get(0).getEncodedNameAsBytes(),tableHRs.get(1).getEncodedNameAsBytes(),true);
            wait(20);
        }
    }

    private void compact() throws IOException, InterruptedException {
        hBaseAdmin.compact(table);
        wait(20);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length!=2){
            System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTable, deleteTable, put, get, getNVersionRow," +
                    " getSpecificColumn, scan, rangeScan, delete, split, move, merge, compact]");
            System.exit(-1);
        }
        HBaseExample hBaseExample = new HBaseExample(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLE")){
            hBaseExample.createTable();
        }
        else if(args[1].toUpperCase().equals("DELETETABLE")){
            hBaseExample.deleteTable();
        }
        else if(args[1].toUpperCase().equals("DELETE")){
            hBaseExample.delete();
        }
        else if(args[1].toUpperCase().equals("PUT")){
            hBaseExample.put();
        }
        else if(args[1].toUpperCase().equals("GET")){
            hBaseExample.get();
        }
        else if(args[1].toUpperCase().equals("GETNVERSIONROW")){
            hBaseExample.getNVersionRow();
        }
        else if(args[1].toUpperCase().equals("GETSPECIFICCOLUMN")){
            hBaseExample.getSpecificColumn();
        }
        else if(args[1].toUpperCase().equals("SCAN")){
            hBaseExample.scan();
        }
        else if(args[1].toUpperCase().equals("RANGESCAN")){
            hBaseExample.rangeScan();
        }
        else if(args[1].toUpperCase().equals("SPLIT")){
            hBaseExample.split();
        }
        else if(args[1].toUpperCase().equals("MOVE")){
            hBaseExample.move();
        }
        else if(args[1].toUpperCase().equals("MERGE")){
            hBaseExample.merge();
        }
        else if(args[1].toUpperCase().equals("COMPACT")){
            hBaseExample.compact();
        }

    }
}
