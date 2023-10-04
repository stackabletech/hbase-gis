package tech.stackable.gis.hbase.coprocessor;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import tech.stackable.gis.hbase.AbstractTestUtil;
import tech.stackable.gis.hbase.generated.KNN;

import static org.junit.Assert.*;

public class TestKNN extends AbstractTestUtil {
    private static final String TABLE = "TestKNN";
    private static final byte[] FAMILY_A = "a".getBytes();
    private static final byte[] X_COL = "lon".getBytes();
    private static final byte[] Y_COL = "lat".getBytes();

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

    @AfterClass
    public static void after() throws Exception {
        HBaseTestingUtility.closeRegionAndWAL(REGION);
    }

    @Test
    /*
    See https://github.com/apache/hbase/blob/rel/2.4.12/hbase-server/src/test/java/org/apache/hadoop/hbase/coprocessor/TestCoprocessorInterface.java#L381
     */
    public void testKNNSingleRegion() throws Exception {
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
        assertEquals(10, response.getPointsCount());

        for (KNN.Point neighbour : response.getPointsList()) {
            LOG.info("Distance to neighbour: " + neighbour.getDistance());
        }
    }
}
