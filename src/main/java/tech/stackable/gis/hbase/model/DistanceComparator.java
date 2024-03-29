package tech.stackable.gis.hbase.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.geom.Point2D;
import java.util.Comparator;

public class DistanceComparator implements Comparator<QueryMatch> {
    static final Logger LOG = LoggerFactory.getLogger(DistanceComparator.class);

    final Point2D origin;

    public DistanceComparator(double lon, double lat) {
        this.origin = new Point2D.Double(lon, lat);
    }

    public int compare(QueryMatch o1, QueryMatch o2) {
        if (Double.isNaN(o1.distance)) {
            o1.distance = origin.distance(o1.lon, o1.lat);
        }
        if (Double.isNaN(o2.distance)) {
            o2.distance = origin.distance(o2.lon, o2.lat);
        }
        if (o1.distance < 0 || o2.distance < 0)
            LOG.warn("negative distance detected!");
        return Double.compare(o1.distance, o2.distance);
    }
}
