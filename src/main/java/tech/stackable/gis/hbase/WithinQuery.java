package tech.stackable.gis.hbase;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import tech.stackable.gis.hbase.filter.WithinFilter;
import tech.stackable.gis.hbase.model.QueryMatch;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class WithinQuery {

    static final byte[] TABLE = "wifi".getBytes();
    static final byte[] FAMILY = "a".getBytes();
    static final byte[] ID = "id".getBytes();
    static final byte[] X_COL = "lon".getBytes();
    static final byte[] Y_COL = "lat".getBytes();

    private static final String usage = "WithinQuery local|remote wkt\n" +
            "  help - print this message and exit.\n" +
            "  local | remote - run the exclusion filter client-side or in the filter.\n" +
            "  wkt - the query geometry in Well-Known Text format.";

    final GeometryFactory factory = new GeometryFactory();
    final Connection pool;

    public WithinQuery(final Connection pool) {
        this.pool = pool;
    }

    Set<Coordinate> getCoords(GeoHash hash) {
        BoundingBox bbox = hash.getBoundingBox();
        Set<Coordinate> coords = new HashSet<>(4);

        double lon = bbox.getSouthEastCorner().getLongitude();
        double lat = bbox.getSouthEastCorner().getLatitude();
        coords.add(new Coordinate(lon, lat));

        lon = bbox.getNorthEastCorner().getLongitude();
        lat = bbox.getNorthEastCorner().getLatitude();
        coords.add(new Coordinate(lon, lat));

        lon = bbox.getNorthWestCorner().getLongitude();
        lat = bbox.getNorthWestCorner().getLatitude();
        coords.add(new Coordinate(lon, lat));

        lon = bbox.getSouthWestCorner().getLongitude();
        lat = bbox.getSouthWestCorner().getLatitude();
        coords.add(new Coordinate(lon, lat));

        return coords;
    }

    Geometry convexHull(GeoHash[] hashes) {
        Set<Coordinate> coords = new HashSet<>();
        for (GeoHash hash : hashes) {
            coords.addAll(getCoords(hash));
        }
        Geometry geom = factory.createMultiPoint(coords.toArray(new Coordinate[0]));
        return geom.convexHull();
    }

    GeoHash[] minimumBoundingPrefixes(Geometry query) {
        GeoHash candidate;
        Geometry candidateGeom;
        Point queryCenter = query.getCentroid();
        for (int precision = 7; precision > 0; precision--) {
            candidate = GeoHash.withCharacterPrecision(queryCenter.getY(),
                    queryCenter.getX(),
                    precision);

            candidateGeom = convexHull(new GeoHash[]{candidate});
            if (candidateGeom.contains(query)) {
                return new GeoHash[]{candidate};
            }

            candidateGeom = convexHull(candidate.getAdjacent());
            if (candidateGeom.contains(query)) {
                GeoHash[] ret = Arrays.copyOf(candidate.getAdjacent(), 9);
                ret[8] = candidate;
                return ret;
            }
        }
        throw new IllegalArgumentException(
                "Geometry cannot be contained by GeoHashs");
    }

    public Set<QueryMatch> query(Geometry query) throws IOException {
        GeoHash[] prefixes = minimumBoundingPrefixes(query);
        Set<QueryMatch> ret = new HashSet<>();
        Table table = pool.getTable(TableName.valueOf(TABLE));

        for (GeoHash prefix : prefixes) {
            byte[] p = prefix.toBase32().getBytes();
            Scan scan = new Scan().withStartRow(p);
            scan.setFilter(new PrefixFilter(p));
            scan.addFamily(FAMILY);
            scan.readVersions(1);
            scan.setCaching(50);

            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                String hash = new String(r.getRow());
                String id = new String(r.getValue(FAMILY, ID));
                String lon = new String(r.getValue(FAMILY, X_COL));
                String lat = new String(r.getValue(FAMILY, Y_COL));
                ret.add(new QueryMatch(id, hash,
                        Double.parseDouble(lon),
                        Double.parseDouble(lat)));
            }
        }

        table.close();

        int exclusionCount = 0;
        for (Iterator<QueryMatch> iter = ret.iterator(); iter.hasNext(); ) {
            QueryMatch candidate = iter.next();
            Coordinate coord = new Coordinate(candidate.lon, candidate.lat);
            Geometry point = factory.createPoint(coord);
            if (!query.contains(point)) {
                iter.remove();
                exclusionCount++;
            }
        }
        System.out.println("Geometry predicate filtered " + exclusionCount + " points.");
        return ret;
    }

    public Set<QueryMatch> queryWithFilter(Geometry query) throws IOException {
        GeoHash[] prefixes = minimumBoundingPrefixes(query);
        Filter withinFilter = new WithinFilter(query, "a".getBytes(), "lat".getBytes(), "lon".getBytes());
        Set<QueryMatch> ret = new HashSet<>();
        Table table = pool.getTable(TableName.valueOf(TABLE));

        for (GeoHash prefix : prefixes) {
            byte[] p = prefix.toBase32().getBytes();
            Filter filters = new FilterList(new PrefixFilter(p), withinFilter);
            Scan scan = new Scan().withStartRow(p);
            scan.setFilter(filters);
            scan.addFamily(FAMILY);
            scan.readVersions(1);
            scan.setCaching(50);

            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                String hash = new String(r.getRow());
                String id = new String(r.getValue(FAMILY, ID));
                String lon = new String(r.getValue(FAMILY, X_COL));
                String lat = new String(r.getValue(FAMILY, Y_COL));
                ret.add(new QueryMatch(id, hash,
                        Double.parseDouble(lon),
                        Double.parseDouble(lat)));
            }
        }

        table.close();
        return ret;
    }

    public static void main(String[] args)
            throws IOException, ParseException {

        if (args.length != 2 || (!"local".equals(args[0]) && !"remote".equals(args[0]))) {
            System.out.println(usage);
            System.exit(0);
        }

        WKTReader reader = new WKTReader();
        Geometry query = reader.read(args[1]);

        final Configuration conf = HBaseConfiguration.create();

        HBaseAdmin.available(conf);

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            long start = System.currentTimeMillis();
            WithinQuery q = new WithinQuery(connection);
            Set<QueryMatch> results;
            if ("local".equals(args[0])) {
                results = q.query(query);
            } else {
                results = q.queryWithFilter(query);
            }
            long end = System.currentTimeMillis();

            for (QueryMatch result : results) {
                System.out.println(result);
            }

            System.out.println(
                    String.format("Query matched %s points in %sms.",
                            results.size(), end - start));
        }
    }
}
