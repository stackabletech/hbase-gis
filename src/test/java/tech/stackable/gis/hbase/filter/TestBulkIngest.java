package tech.stackable.gis.hbase.filter;


import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import tech.stackable.gis.hbase.AbstractTestUtil;
import tech.stackable.gis.hbase.BulkIngest;
import tech.stackable.gis.hbase.model.QueryMatch;

import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestBulkIngest extends AbstractTestUtil {
    public static final String TABLE = "TestWithinFilter";
    private static final byte[] FAMILY = "a".getBytes();

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
            LOG.debug("Put:[{}]", put);
            REGION.put(put);
        }
        REGION.flush(true);

        long end = System.currentTimeMillis();
        LOG.info("Geohashed [{}] bulk ingest records in [{}]ms.", row_count, end - start);

        List<QueryMatch> results = queryWithFilterAndRegionScanner(REGION, new FilterList(), FAMILY, COLUMNS_SCAN);
        // assume no duplicates
        assertEquals(row_count, results.size());

        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-76.0 44.0, " +
                "-76.0 45.0, " +
                "-75.0 45.0, " +
                "-75.0 44.0, " +
                "-76.0 44.0))";

        Geometry query = reader.read(polygon);
        Filter withinFilter = new WithinFilter(query, FAMILY, "lon".getBytes(), "lat".getBytes());
        Filter filters = new FilterList(withinFilter);
        List<QueryMatch> filtered = queryWithFilterAndRegionScanner(REGION, filters, FAMILY, COLUMNS_SCAN);
        assertEquals(row_count, filtered.size());
    }
}
