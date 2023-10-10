package tech.stackable.gis.hbase.coprocessor;

public class Neighbor {
    public final String key;
    public final double lat;
    public final double lon;

    public Neighbor(String key, double lat, double lon) {
        this.key = key;
        this.lat = lat;
        this.lon = lon;
    }
}
