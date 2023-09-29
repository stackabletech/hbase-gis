import ch.hsr.geohash.GeoHash;
import com.google.common.base.Splitter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tech.stackable.gis.hbase.filter.WithinFilter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestWithinFilter extends AbstractTestFilter {
    public static final int POINT_COUNT = 10;
    public static final String TABLE = "TestWithinFilter";
    private static final byte[] FAMILY_A = "a".getBytes();
    private static final byte[] FAMILY_B = "b".getBytes();
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
    public static int WIFI_COUNT;

    @BeforeClass
    public static void before() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
        HColumnDescriptor family_a = new HColumnDescriptor(FAMILY_A).setVersions(100, 100);
        HColumnDescriptor family_b = new HColumnDescriptor(FAMILY_B).setVersions(100, 100);
        htd.addFamily(family_a);
        htd.addFamily(family_b);
        HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

        REGION = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
                TEST_UTIL.getConfiguration(), htd);

        load_wifi_data();
        load_rectangle_check_data();
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

    private static void load_rectangle_check_data() throws Exception {
        // add a dataset consisting of a straight line y=x
        for (Integer i = 1; i <= POINT_COUNT; i++) {
            Put p = new Put(BigInteger.valueOf(i).toByteArray());
            p.setDurability(Durability.SKIP_WAL);
            for (byte[] column : COLUMNS_RECTANGLE_CHECK) {
                p.addColumn(FAMILY_B, column, (i + ".0").getBytes());
            }
            REGION.put(p);
        }

        REGION.flush(true);
    }

    @AfterClass
    public static void after() throws Exception {
        HBaseTestingUtility.closeRegionAndWAL(REGION);
    }

    @Test
    public void testWithoutFilter() throws Exception {
        int results = queryWithFilterAndRegionScanner(REGION, new FilterList(), FAMILY_A, COLUMNS_SCAN);
        assertEquals(WIFI_COUNT, results);
    }

    @Test
    public void testWithPolygonFilter() throws Exception {
        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-73.980844 40.758703, " +
                "-73.987214 40.761369, " +
                "-73.990839 40.756400, " +
                "-73.984422 40.753642, " +
                "-73.980844 40.758703))";

        Geometry query = reader.read(polygon);
        Filter withinFilter = new WithinFilter(query, "wifi".getBytes(), FAMILY_A, "lat".getBytes(), "lon".getBytes());
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(REGION, filters, FAMILY_A, COLUMNS_SCAN);
        assertEquals(26, results);
    }

    @Test
    public void testWithReducedPolygonFilter() throws Exception {
        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-73.980844 40.758703, " +
                "-73.987214 40.761369, " +
                "-73.984422 40.753642, " +
                "-73.980844 40.758703))";

        Geometry query = reader.read(polygon);
        Filter withinFilter = new WithinFilter(query, "wifi".getBytes(), FAMILY_A, "lat".getBytes(), "lon".getBytes());
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(REGION, filters, FAMILY_A, COLUMNS_SCAN);
        assertEquals(10, results);
    }

    @Test
    public void testLinePointsWithoutFilter() throws Exception {
        int results = queryWithFilterAndRegionScanner(REGION, new FilterList(), FAMILY_B, COLUMNS_RECTANGLE_CHECK);
        assertEquals(POINT_COUNT, results);
    }

    @Test
    public void testLinePointsWithRectangleFilter() throws Exception {
        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((0.0 0.0, " +
                "0.0 3.0, " +
                "3.0 3.0, " +
                "3.0 0.0," +
                "0.0 0.0))";

        Geometry query = reader.read(polygon);
        Filter withinFilter = new WithinFilter(query, "wifi".getBytes(), FAMILY_B, "lat".getBytes(), "lon".getBytes());
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(REGION, filters, FAMILY_B, COLUMNS_RECTANGLE_CHECK);
        // excludes points lying on the polygon itself
        assertEquals(2, results);

        // slightly extend polygon to catch the third point
        polygon = "POLYGON ((0.0 0.0, " +
                "0.0 3.0001, " +
                "3.0001 3.0001, " +
                "3.0001 0.0," +
                "0.0 0.0))";
        query = reader.read(polygon);
        withinFilter = new WithinFilter(query, "wifi".getBytes(), FAMILY_B, "lat".getBytes(), "lon".getBytes());
        filters = new FilterList(withinFilter);
        results = queryWithFilterAndRegionScanner(REGION, filters, FAMILY_B, COLUMNS_RECTANGLE_CHECK);
        assertEquals(3, results);
    }
}
