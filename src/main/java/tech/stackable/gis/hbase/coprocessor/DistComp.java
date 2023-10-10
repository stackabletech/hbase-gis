package tech.stackable.gis.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.awt.geom.Point2D;
import java.util.Comparator;

public class DistComp implements Comparator<Neighbor> {
    static final Log LOG = LogFactory.getLog(DistComp.class);
    private final Point2D origin;

    public DistComp(double lat, double lon) {
        this.origin = new Point2D.Double(lon, lat);
    }

    @Override
    public int compare(Neighbor o1, Neighbor o2) {
        final double o1Distance = origin.distance(o1.lon, o1.lat);
        final double o2Distance = origin.distance(o2.lon, o2.lat);
        if (o1Distance < 0 || o2Distance < 0)
            LOG.warn("negative distance detected!");
        return Double.compare(o1Distance, o2Distance);
    }

    public double distance(Neighbor neighbor) {
        return origin.distance(neighbor.lon, neighbor.lat);
    }
}
