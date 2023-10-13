package tech.stackable.gis.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tech.stackable.gis.hbase.generated.KNN;

import java.awt.geom.Point2D;
import java.util.Comparator;

public class DistComp implements Comparator<KNN.Point> {
    static final Log LOG = LogFactory.getLog(DistComp.class);
    private final Point2D origin;

    public DistComp(double lon, double lat) {
        this.origin = new Point2D.Double(lon, lat);
    }

    @Override
    public int compare(KNN.Point o1, KNN.Point o2) {
        if (o1.getDistance() < 0 || o2.getDistance() < 0)
            LOG.warn("negative distance detected!");
        return Double.compare(o1.getDistance(), o2.getDistance());
    }

    public double distance(double lon, double lat) {
        return origin.distance(lon, lat);
    }

}
