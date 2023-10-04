package tech.stackable.gis.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AbstractTestFilter {
    protected final static Logger LOG = LoggerFactory.getLogger(AbstractTestFilter.class);
    protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    protected static HRegion REGION;

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
