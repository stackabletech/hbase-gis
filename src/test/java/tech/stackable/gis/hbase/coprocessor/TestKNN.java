package tech.stackable.gis.hbase.coprocessor;

import ch.hsr.geohash.GeoHash;
import com.google.common.base.Splitter;
import com.google.protobuf.ByteString;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.stackable.gis.hbase.generated.KNN;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class TestKNN {
    private final static Logger LOG = LoggerFactory.getLogger(TestKNN.class);
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static HRegion REGION;
    private static final String TABLE = "TestKNN";
    private static final byte[] FAMILY_A = "a".getBytes();
    private static final String[] COLUMNS = new String[]{
            "lon", "lat", "id", "name", "address",
            "city", "url", "phone", "type", "zip"};
    private static final ArrayIterator COLS = new ArrayIterator(COLUMNS);
    private static final Splitter SPLITTER = Splitter.on('\t')
            .trimResults()
            .limit(COLUMNS.length);
    private static final byte[] X_COL = "lon".getBytes();
    private static final byte[] Y_COL = "lat".getBytes();
    private static final byte[][] COLUMNS_SCAN = {"id".getBytes(), X_COL, Y_COL};
    private static final byte[][] COLUMNS_RECTANGLE_CHECK = {X_COL, Y_COL};
    private static int WIFI_COUNT;

    @BeforeClass
    public static void before() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
        htd.addCoprocessor("");
        HColumnDescriptor family_a = new HColumnDescriptor(FAMILY_A).setVersions(100, 100);
        htd.addFamily(family_a);
        HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

        REGION = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
                TEST_UTIL.getConfiguration(), htd);

        load_wifi_data();
    }

    private static void load_wifi_data() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/wifi_4326.txt"));
        reader.readLine(); // ignore header
        String line;

        int records = 0, duplicates = 0;
        Set<String> uniqueKeys = new HashSet<>();
        long start = System.currentTimeMillis();

        try {
            while ((line = reader.readLine()) != null) {
                COLS.reset();
                Iterator<String> vals = SPLITTER.split(line).iterator();
                Map<String, String> row = new HashMap<>(COLUMNS.length);

                while (vals.hasNext() && COLS.hasNext()) {
                    String col = (String) COLS.next();
                    String val = vals.next();
                    row.put(col, val);
                }

                double lat = Double.parseDouble(row.get("lat"));
                double lon = Double.parseDouble(row.get("lon"));
                String rowkey = GeoHash.withCharacterPrecision(lat, lon, 12).toBase32();
                // ignore duplicates
                if (!uniqueKeys.contains(rowkey)) {
                    uniqueKeys.add(rowkey);
                    Put put = new Put(rowkey.getBytes());
                    put.setDurability(Durability.SKIP_WAL);
                    for (Map.Entry<String, String> e : row.entrySet()) {
                        put.addColumn(FAMILY_A, e.getKey().getBytes(), e.getValue().getBytes());
                    }
                    REGION.put(put);
                    records++;
                } else {
                    duplicates++;
                }
            }
        } finally {
            reader.close();
        }

        REGION.flush(true);

        long end = System.currentTimeMillis();
        WIFI_COUNT = records;
        LOG.info(String.format("Geohashed %s records (%s duplicates) in %sms.", records, duplicates, end - start));
    }

    @AfterClass
    public static void after() throws Exception {
        HBaseTestingUtility.closeRegionAndWAL(REGION);
    }

    @Test
    public void testKNN() throws Exception {
        Configuration conf = TEST_UTIL.getConfiguration();
        RegionCoprocessorHost host = new RegionCoprocessorHost(REGION, Mockito.mock(RegionServerServices.class), conf);
        REGION.setCoprocessorHost(host);
        host.load(KNNEndpoint.class.asSubclass(RegionCoprocessor.class), Coprocessor.PRIORITY_USER, conf);
        host.preOpen();
        host.postOpen();

        // check coprocessor was loaded
        assertNotNull(host.findCoprocessor(KNNEndpoint.class.getName()));

        Coprocessor c = REGION.getCoprocessorHost().findCoprocessor(KNNEndpoint.class);
        assertTrue(c instanceof KNNEndpoint);
        KNNEndpoint knn = (KNNEndpoint) c;

        var latArg = -73.97000655;
        var lonArg = 40.76098703;

        KNN.KNNRequest request = KNN.KNNRequest.newBuilder()
                .setCount(10)
                .setLat(latArg)
                .setLon(lonArg)
                .setFamily(ByteString.copyFrom(FAMILY_A))
                .setLatCol(ByteString.copyFrom(Y_COL))
                .setLonCol(ByteString.copyFrom(X_COL))
                .build();

        BlockingRpcCallback<KNN.KNNResponse> rpcCallback = new BlockingRpcCallback<>();
        knn.getKNN(null, request, rpcCallback);
        KNN.KNNResponse response = rpcCallback.get();

        // a single region, so we should get the same count back
        assertEquals(10, response.getKeysList().size());
    }
}
