import HBaseIA.GIS.filter.WithinFilter;
import ch.hsr.geohash.GeoHash;
import com.google.common.base.Splitter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestWithinFilter {
    private final static Logger LOG = LoggerFactory.getLogger(TestWithinFilter.class);
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    public static final int POINT_COUNT = 10;

    private static HRegion REGION;
    private static final byte[] FAMILY = "a".getBytes();
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

    //private static final byte[] FAMILY_RECTANGLE_CHECK = "r".getBytes();
    private static final byte[][] COLUMNS_RECTANGLE_CHECK = {X_COL, Y_COL};

    @BeforeClass
    public static void before() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestWithinFilter"));
        HColumnDescriptor family = new HColumnDescriptor(FAMILY).setVersions(100, 100);
        //HColumnDescriptor family_rectangle_check = new HColumnDescriptor(FAMILY).setVersions(100, 100);
        htd.addFamily(family);
        //htd.addFamily(family_rectangle_check);
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

        int records = 0;
        long start = System.currentTimeMillis();

        try {
            while ((line = reader.readLine()) != null) {
                COLS.reset();
                Iterator<String> vals = SPLITTER.split(line).iterator();
                Map<String, String> row = new HashMap<String, String>(COLUMNS.length);

                while (vals.hasNext() && COLS.hasNext()) {
                    String col = (String) COLS.next();
                    String val = vals.next();
                    row.put(col, val);
                }

                double lat = Double.parseDouble(row.get("lat"));
                double lon = Double.parseDouble(row.get("lon"));
                String rowkey = GeoHash.withCharacterPrecision(lat, lon, 12).toBase32();
                Put put = new Put(rowkey.getBytes());
                put.setDurability(Durability.SKIP_WAL);
                for (Map.Entry<String, String> e : row.entrySet()) {
                    put.addColumn(FAMILY, e.getKey().getBytes(), e.getValue().getBytes());
                }
                REGION.put(put);
                records++;
            }
        } finally {
            reader.close();
        }

        REGION.flush(true);

        long end = System.currentTimeMillis();
        LOG.debug(String.format("Geohashed %s records in %sms.", records, end - start));
    }

    private static void load_rectangle_check_data() throws Exception {
        // add a dataset consisting of a straight line y=x
        for (Integer i = 1; i <= POINT_COUNT; i++) {
            Put p = new Put(BigInteger.valueOf(i).toByteArray());
            p.setDurability(Durability.SKIP_WAL);
            for (byte[] column : COLUMNS_RECTANGLE_CHECK) {
                p.addColumn(FAMILY, column, (i +".0").getBytes());
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
        int results = queryWithFilterAndRegionScanner(REGION, new FilterList(), FAMILY, COLUMNS_SCAN);
        assertEquals(1234, results);
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
        Filter withinFilter = new WithinFilter(query);
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(REGION, filters, FAMILY, COLUMNS_SCAN);
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
        Filter withinFilter = new WithinFilter(query);
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(REGION, filters, FAMILY, COLUMNS_SCAN);
        assertEquals(10, results);
    }

    @Test
    public void testLinePointsWithoutFilter() throws Exception {
        int results = queryWithFilterAndRegionScanner(REGION, new FilterList(), FAMILY, COLUMNS_RECTANGLE_CHECK);
        assertEquals(1234, results);
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
        Filter withinFilter = new WithinFilter(query);
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(REGION, filters, FAMILY, COLUMNS_RECTANGLE_CHECK);
        assertEquals(2, results);
    }

    private int queryWithFilterAndRegionScanner(Region region, Filter filters, byte[] family, byte[][] columnsScan) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(filters);
        scan.addFamily(family);
        scan.readVersions(1);
        scan.setCaching(50);
        for (byte[] column : columnsScan) {
            scan.addColumn(family, column);
        }

        InternalScanner scanner = region.getScanner(scan);
        int i = 0;

        while (true) {
            StringBuilder sb = new StringBuilder();
            List<Cell> results = new ArrayList<>();
            scanner.next(results);

            Arrays.sort(results.toArray(new Cell[results.size()]), CellComparator.getInstance());

            if (!results.isEmpty()) {
                i++;
                sb.append("Columns -");

                for (Cell cell : results) {
                    sb.append(" ");
                    sb.append(Bytes.toString(CellUtil.cloneQualifier(cell)));
                    sb.append(":");
                    sb.append(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                LOG.debug(sb.toString());
            } else {
                break;
            }
        }
        LOG.info(String.format("%s Rows found.", i));
        return i;
    }
}
