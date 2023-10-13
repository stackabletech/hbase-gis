package tech.stackable.gis.hbase.coprocessor;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import tech.stackable.gis.hbase.AbstractTestUtil;
import tech.stackable.gis.hbase.generated.KNN;
import tech.stackable.gis.hbase.model.QueryMatch;

import java.awt.geom.Point2D;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

        KNNEndpoint knn = REGION.getCoprocessorHost().findCoprocessor(KNNEndpoint.class);

        var latArg = -73.97000655;
        var lonArg = 40.76098703;

        int topX = 10;
        KNN.KNNRequest request = KNN.KNNRequest.newBuilder()
                .setCount(topX)
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
        assertEquals(topX, response.getPointsCount());

        // compare to the dataset
        List<QueryMatch> results = queryWithFilterAndRegionScanner(REGION, new FilterList(), FAMILY_A, COLUMNS_SCAN);
        final var origin = new Point2D.Double(lonArg, latArg);
        Set<Double> distances = Sets.newTreeSet();
        Set<Double> neighbourDistances = Sets.newTreeSet();

        for (KNN.Point neighbour : response.getPointsList()) {
            neighbourDistances.add(neighbour.getDistance());
        }
        for (QueryMatch result : results) {
            distances.add(origin.distance(result.lon, result.lat));
        }
        Iterator<Double> iterator = distances.iterator();
        // compare both sets of top-X
        for (Double d : neighbourDistances) {
            LOG.info("Distance to neighbour: [{}]", d);
            assertEquals(d, iterator.next());
        }
    }
}
