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
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestWithinFilter {
    private final static Logger LOG = LoggerFactory.getLogger(TestWithinFilter.class);
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    private static HRegion REGION;
    private static final byte[] FAMILY = "a".getBytes();
    private static final String[] COLUMNS = new String[]{
            "lon", "lat", "id", "name", "address",
            "city", "url", "phone", "type", "zip"};
    private static final ArrayIterator COLS = new ArrayIterator(COLUMNS);
    private static final Splitter SPLITTER = Splitter.on('\t')
            .trimResults()
            .limit(COLUMNS.length);

    private static final byte[] ID = "id".getBytes();
    private static final byte[] X_COL = "lon".getBytes();
    private static final byte[] Y_COL = "lat".getBytes();

    @BeforeClass
    public static void before() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestWithinFilter"));
        HColumnDescriptor family = new HColumnDescriptor(FAMILY).setVersions(100, 100);
        htd.addFamily(family);
        HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

        REGION = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
                TEST_UTIL.getConfiguration(), htd);

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

    @AfterClass
    public static void after() throws Exception {
        HBaseTestingUtility.closeRegionAndWAL(REGION);
    }

    @Test
    public void testScanAll() throws Exception {
        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-73.980844 40.758703, " +
                "-73.987214 40.761369, " +
                "-73.990839 40.756400, " +
                "-73.984422 40.753642, " +
                "-73.980844 40.758703))";

        Geometry query = reader.read(polygon);
        int results = queryWithFilterAndRegionScanner(query, REGION, new FilterList());
        assertEquals(1224, results);
    }

    @Test
    public void testScanWithFilter() throws Exception {
        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-73.980844 40.758703, " +
                "-73.987214 40.761369, " +
                "-73.990839 40.756400, " +
                "-73.984422 40.753642, " +
                "-73.980844 40.758703))";

        Geometry query = reader.read(polygon);
        Filter withinFilter = new WithinFilter(query);
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(query, REGION, filters);
        assertEquals(26, results);
    }

    @Test
    public void testRectangleScanWithFilter() throws Exception {
        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-73.980844 40.758703, " +
                "-73.987214 40.761369, " +
                "-73.984422 40.753642, " +
                "-73.980844 40.758703))";

        Geometry query = reader.read(polygon);
        Filter withinFilter = new WithinFilter(query);
        Filter filters = new FilterList(withinFilter);
        int results = queryWithFilterAndRegionScanner(query, REGION, filters);
        assertEquals(10, results);
    }

    private int queryWithFilterAndRegionScanner(Geometry query, Region region, Filter filters) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(filters);
        scan.addFamily(FAMILY);
        scan.readVersions(1);
        scan.setCaching(50);
        scan.addColumn(FAMILY, ID);
        scan.addColumn(FAMILY, X_COL);
        scan.addColumn(FAMILY, Y_COL);

        InternalScanner scanner = region.getScanner(scan);
        int i = 0;

        while (true) {
            StringBuilder sb = new StringBuilder();
            List<Cell> results = new ArrayList<>();
            scanner.next(results);

            Arrays.sort(results.toArray(new Cell[results.size()]), CellComparator.getInstance());

            if (!results.isEmpty()) {
                i++;
                sb.append("Row:");

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
