package tech.stackable.gis.hbase;

import ch.hsr.geohash.GeoHash;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import tech.stackable.gis.hbase.model.DistanceComparator;
import tech.stackable.gis.hbase.model.QueryMatch;

import java.io.IOException;
import java.util.Comparator;
import java.util.Queue;

public class KNNQuery {

    static final byte[] TABLE = "wifi".getBytes();
    static final byte[] FAMILY = "a".getBytes();
    static final byte[] ID = "id".getBytes();
    static final byte[] X_COL = "lon".getBytes();
    static final byte[] Y_COL = "lat".getBytes();

    private static final String usage = "KNNQuery lon lat n\n" +
            "  help - print this message and exit.\n" +
            "  lon, lat - query position.\n" +
            "  n - the number of neighbors to return.";

    final Connection connection;
    int precision = 7;

    public KNNQuery(final Connection connection) {
        this.connection = connection;
    }

    public KNNQuery(Connection connection, int characterPrecision) {
        this.connection = connection;
        this.precision = characterPrecision;
    }

    Queue<QueryMatch> takeN(Comparator<QueryMatch> comp,
                            String prefix,
                            int n) throws IOException {
        Queue<QueryMatch> candidates = MinMaxPriorityQueue.orderedBy(comp)
                .maximumSize(n)
                .create();

        Scan scan = new Scan().withStartRow(prefix.getBytes());
        scan.setFilter(new PrefixFilter(prefix.getBytes()));
        scan.addFamily(FAMILY);
        scan.readVersions(1);
        scan.setCaching(50);

        Table table = connection.getTable(TableName.valueOf(TABLE));

        int cnt = 0;
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            String hash = new String(r.getRow());
            String id = new String(r.getValue(FAMILY, ID));
            String lon = new String(r.getValue(FAMILY, X_COL));
            String lat = new String(r.getValue(FAMILY, Y_COL));
            candidates.add(new QueryMatch(id, hash,
                    Double.parseDouble(lon),
                    Double.parseDouble(lat)));
            cnt++;
        }

        table.close();

        System.out.printf("Scan over '%s' returned %s candidates.%n",
                prefix, cnt);
        return candidates;
    }

    public Queue<QueryMatch> queryKNN(double lat, double lon, int n)
            throws IOException {
        DistanceComparator comp = new DistanceComparator(lon, lat);
        Queue<QueryMatch> ret = MinMaxPriorityQueue.orderedBy(comp)
                .maximumSize(n)
                .create();

        GeoHash target = GeoHash.withCharacterPrecision(lat, lon, precision);
        ret.addAll(takeN(comp, target.toBase32(), n));
        for (GeoHash h : target.getAdjacent()) {
            ret.addAll(takeN(comp, h.toBase32(), n));
        }

        return ret;
    }

    public static void main(String[] args) throws IOException {

        if (args.length != 3) {
            System.out.println(usage);
            System.exit(0);
        }

        double lon = Double.parseDouble(args[0]);
        double lat = Double.parseDouble(args[1]);
        int n = Integer.parseInt(args[2]);

        final Configuration conf = HBaseConfiguration.create();

        HBaseAdmin.available(conf);

        try (Connection connection = ConnectionFactory.createConnection(conf)) {

            KNNQuery q = new KNNQuery(connection);
            Queue<QueryMatch> ret = q.queryKNN(lat, lon, n);

            QueryMatch m;
            while ((m = ret.poll()) != null) {
                System.out.println(m);
            }
        }
    }
}
