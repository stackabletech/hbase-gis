import HBaseIA.GIS.filter.WithinFilter;
import HBaseIA.GIS.model.QueryMatch;
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
import org.apache.hadoop.hbase.filter.TestFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class TestWithinFilter {
    private final static Logger LOG = LoggerFactory.getLogger(TestFilter.class);
    private HRegion region;

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
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

//    @BeforeClass
//    public static void setUpBeforeClass() throws Exception {
//        StartMiniClusterOption option = StartMiniClusterOption.builder().numMasters(1)
//                .numRegionServers(0).numDataNodes(1).numZkServers(1).createRootDir(true).build();
//
//        TEST_UTIL.getConfiguration().addResource("hbase-site-local.xml");
//        TEST_UTIL.getConfiguration().reloadConfiguration();
//        //TEST_UTIL.getConfiguration().set();
//
//        TEST_UTIL.startMiniCluster(option);
//        //TEST_UTIL.startMiniCluster(1);
//    }
//
//    @AfterClass
//    public static void tearDownAfterClass() throws Exception {
//        TEST_UTIL.shutdownMiniCluster();
//    }

    @Before
    public void setup() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestFilter"));
        HColumnDescriptor family = new HColumnDescriptor(FAMILY).setVersions(100, 100);
        htd.addFamily(family);
        HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

        this.region = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
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
                this.region.put(put);
                records++;
            }
        } finally {
            reader.close();
        }

        this.region.flush(true);

        long end = System.currentTimeMillis();
        System.out.println(String.format("Geohashed %s records in %sms.", records, end - start));
    }

    @After
    public void tearDown() throws Exception {
        HBaseTestingUtility.closeRegionAndWAL(region);
    }

    @Test
    public void testFilter() throws Exception {
        WKTReader reader = new WKTReader();

        String polygon = "POLYGON ((-73.980844 40.758703, " +
                "-73.987214 40.761369, " +
                "-73.990839 40.756400, " +
                "-73.984422 40.753642, " +
                "-73.980844 40.758703))";

        Geometry query = reader.read(polygon);

//        WithinQuery q = new WithinQuery(ConnectionFactory.createConnection(TEST_UTIL.getConfiguration()));

        Set<QueryMatch> results = queryWithFilterAndRegionScanner(query, this.region);

//        if ("local".equals(mode)) {
//            results = q.query(query);
//        } else {
//            results = q.queryWithFilter(query);
//        }
//
//        System.out.println("Query matched " + results.size() + " points.");
//        for (QueryMatch result : results) {
//            System.out.println(result);
//        }

    }

    private Set<QueryMatch> queryWithFilterAndRegionScanner(Geometry query, Region region) throws IOException {
        Filter withinFilter = new WithinFilter(query);
        Set<QueryMatch> ret = new HashSet<QueryMatch>();

        Filter filters = new FilterList(withinFilter);
        Scan scan = new Scan();
        //scan.setFilter(filters);
        scan.addFamily(FAMILY);
        scan.readVersions(1);
        scan.setCaching(50);
        scan.addColumn(FAMILY, ID);
        scan.addColumn(FAMILY, X_COL);
        scan.addColumn(FAMILY, Y_COL);

        List<Cell> results = new ArrayList<>();
        InternalScanner scanner = region.getScanner(scan);
        scanner.next(results);

        Arrays.sort(results.toArray(new Cell[results.size()]), CellComparator.getInstance());

        for (Cell cell : results) {
            System.out.println("Matching: " + Bytes.toString(cell.getRowArray()) + "/" + Bytes.toString(cell.getQualifierArray()));
        }

//      ResultScanner scanner = table.getScanner(scan);
//      for (Result r : scanner) {
//        String hash = new String(r.getRow());
//        String id = new String(r.getValue(FAMILY, ID));
//        String lon = new String(r.getValue(FAMILY, X_COL));
//        String lat = new String(r.getValue(FAMILY, Y_COL));
//        ret.add(new QueryMatch(id, hash,
//                Double.parseDouble(lon),
//                Double.parseDouble(lat)));
//      }

        return ret;
    }
}
