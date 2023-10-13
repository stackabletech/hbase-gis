package tech.stackable.gis.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.awt.geom.Point2D;
import java.util.Comparator;

public class DistComp implements Comparator<Neighbor> {
    static final Log LOG = LogFactory.getLog(DistComp.class);
    private final Point2D origin;

    public DistComp(double lon, double lat) {
        this.origin = new Point2D.Double(lon, lat);
    }

    @Override
    public int compare(Neighbor o1, Neighbor o2) {
        if (o1.distance < 0 || o2.distance < 0)
            LOG.warn("negative distance detected!");
        return Double.compare(o1.distance, o2.distance);
    }

    public double distance(double lon, double lat) {
        return origin.distance(lon, lat);
    }
}
