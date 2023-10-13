package tech.stackable.gis.hbase.coprocessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
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
import org.apache.hbase.thirdparty.com.google.common.collect.MinMaxPriorityQueue;
import tech.stackable.gis.hbase.generated.KNN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KNNEndpoint extends KNN.KNNService implements RegionCoprocessor, CoprocessorService {
    static final Log LOG = LogFactory.getLog(KNNEndpoint.class);

    private RegionCoprocessorEnvironment env;

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
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
    public void getKNN(RpcController controller, KNN.KNNRequest request, RpcCallback<KNN.KNNResponse> done) {

        final byte[] family = request.getFamily().toByteArray();
        final byte[] latCol = request.getLatCol().toByteArray();
        final byte[] lonCol = request.getLonCol().toByteArray();
        final int count = request.getCount();

        Scan scan = new Scan();
        scan.addFamily(family);
        scan.addColumn(family, latCol);
        scan.addColumn(family, lonCol);

        KNN.KNNResponse response = null;
        InternalScanner scanner = null;
        final var distComp = new DistComp(request.getLon(), request.getLat());
        // TODO replace with blocking priority queue and keep count of items and look up max value
        final MinMaxPriorityQueue<KNN.Point> knn = MinMaxPriorityQueue.orderedBy(distComp).maximumSize(count).create();
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<>();
            boolean hasMore;

            do {
                var lon = Double.NaN;
                var lat = Double.NaN;
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    if (CellUtil.matchingColumn(cell, family, lonCol))
                        lon = parseCoordinate(cell);
                    if (CellUtil.matchingColumn(cell, family, latCol))
                        lat = parseCoordinate(cell);

                    if (!Double.isNaN(lat) && !Double.isNaN(lon)) {
                        double distance = distComp.distance(lon, lat);
                        if (knn.size() < count || distComp.distance(lon, lat) < knn.peekLast().getDistance()) {
                            knn.add(KNN.Point.newBuilder().setKey(ByteString.copyFromUtf8(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())))
                                    .setLon(lon)
                                    .setLat(lat)
                                    .setDistance(distance).build());
                        }
                    }
                }
                results.clear();
            } while (hasMore);

            response = KNN.KNNResponse.newBuilder().addAllPoints(knn).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                }
            }
        }

        done.run(response);
    }

    final Double parseCoordinate(final Cell cell) {
        byte[] value;
        int offset;
        int len;
        if (cell instanceof ByteBufferExtendedCell) {
            value = ((ByteBufferExtendedCell) cell).getValueByteBuffer().array();
            offset = ((ByteBufferExtendedCell) cell).getValuePosition();
        } else {
            value = cell.getValueArray();
            offset = cell.getValueOffset();
        }
        len = cell.getValueLength();
        try {
            return Double.parseDouble(Bytes.toString(value, offset, len));
        } catch (NumberFormatException nfe) {
            LOG.error(String.format("Failed to parse coordinate for key %s", CellUtil.getCellKeyAsString(cell)));
        }
        return Double.NaN;
    }

}
