package tech.stackable.gis.hbase.coprocessor;

public class Neighbor {
    public final String key;
    public final double lat;
    public final double lon;
    public final double distance;

    public Neighbor(String key, double lon, double lat, double distance) {
        this.key = key;
        this.lon = lon;
        this.lat = lat;
        this.distance = distance;
    }
}
