package tech.stackable.gis.hbase;

import ch.hsr.geohash.GeoHash;
import com.google.common.base.Splitter;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class AbstractTestUtil {
    protected final static Logger LOG = LoggerFactory.getLogger(AbstractTestUtil.class);
    protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    protected static HRegion REGION;
    protected static final byte[] FAMILY_A = "a".getBytes();
    protected static int WIFI_COUNT;

    private static final String[] COLUMNS = new String[]{
            "lon", "lat", "id", "name", "address",
            "city", "url", "phone", "type", "zip"};
    private static final ArrayIterator COLS = new ArrayIterator(COLUMNS);
    private static final Splitter SPLITTER = Splitter.on('\t')
            .trimResults()
            .limit(COLUMNS.length);

    protected static void load_wifi_data() throws IOException {
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

    protected static int queryWithFilterAndRegionScanner(Region region, Filter filters, byte[] family, byte[][] columnsScan) throws IOException {
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

            Arrays.sort(results.toArray(new Cell[0]), CellComparator.getInstance());

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
