package tech.stackable.gis.hbase.coprocessor;

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
import tech.stackable.gis.hbase.AbstractTestUtil;
import tech.stackable.gis.hbase.generated.TopX;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestTopX {
    protected final static Logger LOG = LoggerFactory.getLogger(AbstractTestUtil.class);
    protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    protected static HRegion REGION;

    private static final String TABLE = "TestTopX";
    private static final byte[] FAMILY = "a".getBytes();

    private static final String[] COLUMNS = new String[]{
            "vendor_id", "pu_ts", "do_ts", "p_count", "trip", "ratecode_id", "flag", "pu_id",
            "do_id", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "imp_surcharge", "total_amount", "cong_surcharge"};
    private static final ArrayIterator COLS = new ArrayIterator(COLUMNS);
    private static final Splitter SPLITTER = Splitter.on(',')
            .trimResults()
            .limit(COLUMNS.length);
    protected static int TRIP_COUNT;

    @BeforeClass
    public static void before() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
        htd.addCoprocessor("");
        HColumnDescriptor family_a = new HColumnDescriptor(FAMILY).setVersions(100, 100);
        htd.addFamily(family_a);
        HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

        REGION = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
                TEST_UTIL.getConfiguration(), htd);

        load_taxi_data();
    }

    protected static void load_taxi_data() throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/yellow_tripdata_2021-07.csv"));
        reader.readLine(); // consume header
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

                String rowkey = row.get("pu_ts") + "|" + row.get("do_ts");
                // ignore duplicates
                if (!uniqueKeys.contains(rowkey)) {
                    uniqueKeys.add(rowkey);
                    Put put = new Put(rowkey.getBytes());
                    put.setDurability(Durability.SKIP_WAL);
                    for (Map.Entry<String, String> e : row.entrySet()) {
                        // convert timestamps to longs
                        String key = e.getKey();
                        String value = e.getValue();
                        if (key.equals("pu_ts") || key.equals("do_ts")) {
                            value = String.valueOf(Timestamp.valueOf(LocalDateTime.parse(value, formatter)).getTime());
                        }
                        put.addColumn(FAMILY, key.getBytes(), value.getBytes());
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
        TRIP_COUNT = records;
        LOG.info("Geohashed [{}] records ([{}] duplicates) in [{}]ms.", records, duplicates, end - start);
    }

    @AfterClass
    public static void after() throws Exception {
        HBaseTestingUtility.closeRegionAndWAL(REGION);
    }

    @Test
    /*
    See https://github.com/apache/hbase/blob/rel/2.4.12/hbase-server/src/test/java/org/apache/hadoop/hbase/coprocessor/TestCoprocessorInterface.java#L381
     */
    public void testTopXSingleRegion() throws Exception {
        Configuration conf = TEST_UTIL.getConfiguration();
        RegionCoprocessorHost host = new RegionCoprocessorHost(REGION, Mockito.mock(RegionServerServices.class), conf);
        REGION.setCoprocessorHost(host);
        host.load(TopXEndpoint.class.asSubclass(RegionCoprocessor.class), Coprocessor.PRIORITY_USER, conf);
        host.preOpen();
        host.postOpen();

        assertNotNull(host.findCoprocessor(TopXEndpoint.class.getName()));
        TopXEndpoint endpoint = REGION.getCoprocessorHost().findCoprocessor(TopXEndpoint.class);

        TopX.TopXResponse response = getTopXResponse(endpoint, 2);
        LOG.info("Unique references [{}]", response.getCandidatesCount());
        assertEquals(207, response.getCandidatesCount());

        // return all rows, should match number imported
        response = getTopXResponse(endpoint, TRIP_COUNT);
        LOG.info("Unique references [{}]", response.getCandidatesCount());
        assertEquals(TRIP_COUNT, response.getCandidatesCount());

        // check a specific key
        int count = 0;
        for (TopX.Candidate candidate : response.getCandidatesList()) {
            if (Integer.parseInt(new String(candidate.getKey().toByteArray())) == 68) {
                count++;
            }
        }
        assertEquals(141, count);
    }

    private TopX.TopXResponse getTopXResponse(TopXEndpoint endpoint, int topX) throws IOException {
        TopX.TopXRequest request = TopX.TopXRequest.newBuilder()
                .setCount(topX)
                .setFamily(ByteString.copyFrom(FAMILY))
                .setReferenceCol(ByteString.copyFrom("pu_id".getBytes()))
                .setTimestampCol(ByteString.copyFrom("pu_ts".getBytes()))
                .build();

        BlockingRpcCallback<TopX.TopXResponse> rpcCallback = new BlockingRpcCallback<>();
        endpoint.getTopX(null, request, rpcCallback);
        TopX.TopXResponse response = rpcCallback.get();
        return response;
    }
}
