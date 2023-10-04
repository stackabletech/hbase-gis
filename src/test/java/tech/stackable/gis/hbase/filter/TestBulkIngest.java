package tech.stackable.gis.hbase.filter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tech.stackable.gis.hbase.BulkIngest;

import java.util.PrimitiveIterator;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestBulkIngest extends AbstractTestFilter {
    public static final String TABLE = "TestWithinFilter";
    private static final byte[] FAMILY = "a".getBytes();
    private static final byte[] X_COL = "lon".getBytes();
    private static final byte[] Y_COL = "lat".getBytes();

    private static final byte[][] COLUMNS_SCAN = {"id".getBytes(), X_COL, Y_COL};

    @BeforeClass
    public static void before() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
        HColumnDescriptor family = new HColumnDescriptor(FAMILY).setVersions(100, 100);
        htd.addFamily(family);
        HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

        REGION = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
                TEST_UTIL.getConfiguration(), htd);
    }

    @AfterClass
    public static void after() throws Exception {
        HBaseTestingUtility.closeRegionAndWAL(REGION);
    }

    @Test
    public void testBulkIngest() throws Exception {
        int row_count = 1000;
        long start = System.currentTimeMillis();

        // generate random co-ordinates within a fixed region
        PrimitiveIterator.OfDouble lon_iter = new Random().doubles(-75.99, -75.01).iterator();
        PrimitiveIterator.OfDouble lat_iter = new Random().doubles(44.01, 44.99).iterator();

        for (int i = 0; i < row_count; i++) {
            Put put = BulkIngest.getPut(FAMILY, i, lon_iter, lat_iter);
            LOG.debug("Put:{}", put);
            REGION.put(put);
        }
        REGION.flush(true);

        long end = System.currentTimeMillis();
        LOG.info(String.format("Geohashed %s bulk ingest records in %sms.", row_count, end - start));

        int results = queryWithFilterAndRegionScanner(REGION, new FilterList(), FAMILY, COLUMNS_SCAN);
        // assume no duplicates
        assertEquals(row_count, results);

        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-76.0 44.0, " +
                "-76.0 45.0, " +
                "-75.0 45.0, " +
                "-75.0 44.0, " +
                "-76.0 44.0))";

        Geometry query = reader.read(polygon);
        Filter withinFilter = new WithinFilter(query, "bulk".getBytes(), FAMILY, "lat".getBytes(), "lon".getBytes());
        Filter filters = new FilterList(withinFilter);
        int filtered = queryWithFilterAndRegionScanner(REGION, filters, FAMILY, COLUMNS_SCAN);
        assertEquals(row_count, filtered);
    }
}
