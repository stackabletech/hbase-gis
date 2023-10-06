package tech.stackable.gis.hbase.coprocessor;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Bytes;
import tech.stackable.gis.hbase.generated.TopX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TopXEndpoint extends TopX.TopXService implements RegionCoprocessor, CoprocessorService {
    static final Log LOG = LogFactory.getLog(TopXEndpoint.class);

    private RegionCoprocessorEnvironment env;

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // do nothing
    }

    @Override
    public void getTopX(RpcController controller, TopX.TopXRequest request, RpcCallback<TopX.TopXResponse> done) {
        final byte[] family = request.getFamily().toByteArray();
        final byte[] referenceCol = request.getReferenceCol().toByteArray();
        final byte[] tsCol = request.getTimestampCol().toByteArray();
        final int count = request.getCount();

        // note each unique value of the reference column
        Scan scan = new Scan();
        scan.addFamily(family);
        scan.addColumn(family, referenceCol);
        scan.readVersions(1);

        Map<String, TopXQueue> uniqueVals = new HashMap<>();
        TopX.TopXResponse response = null;
        try (InternalScanner scanner = env.getRegion().getScanner(scan)) {
            List<Cell> results = new ArrayList<>();
            boolean hasMore, hasMoreItems;

            do {
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    if (CellUtil.matchingColumn(cell, family, referenceCol)) {
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        if (!uniqueVals.containsKey(value)) {
                            uniqueVals.put(value, new TopXQueue(count));
                        }
                        // scan for this entry
                        Scan itemScan = new Scan();
                        itemScan.addFamily(family);
                        itemScan.addColumn(family, referenceCol);
                        InternalScanner itemScanner = env.getRegion().getScanner(itemScan);
                        List<Cell> itemResults = new ArrayList<>();
                        TopXQueue cachedItems = uniqueVals.get(value);
                        do {
                            hasMoreItems = itemScanner.next(itemResults);
                            var ts = Double.NaN;
                            String ref = null;
                            for (Cell itemCell : itemResults) {
                                if (CellUtil.matchingColumn(itemCell, family, referenceCol)) {
                                    ref = Bytes.toString(CellUtil.cloneValue(cell));
                                    if (!ref.equals(value)) {
                                        break;
                                    }
                                }
                                if (CellUtil.matchingColumn(itemCell, family, tsCol)) {
                                    ts = Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell)));
                                }
                                if (ref != null && !Double.isNaN(ts)) {
                                    cachedItems.add(new Item(CellUtil.getCellKeyAsString(itemCell), ref, ts));
                                }
                            }
                            itemResults.clear();
                        } while (hasMoreItems);
                    }
                }
                results.clear();
            } while (hasMore);

            var resBuilder = TopX.TopXResponse.newBuilder();

            for (Map.Entry<String, TopXQueue> itemQueue : uniqueVals.entrySet()) {
                for (Item item : itemQueue.getValue()) {
                    resBuilder.addCandidates(TopX.Candidate.newBuilder().setKey(ByteString.copyFrom(itemQueue.getKey(), Charsets.UTF_8))
                            .setReference(ByteString.copyFrom(item.item, Charsets.UTF_8))
                            .setTimestamp(item.timestamp));
                }
            }
            response = resBuilder.build();

        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        }

        done.run(response);
    }
}