package tech.stackable.gis.hbase.coprocessor;

public class Item implements Comparable<Item> {
    public final String key;
    public final String item;
    public final double timestamp;

    public Item(String key, String item, double timestamp) {
        this.key = key;
        this.item = item;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(Item item) {
        return Double.compare(this.timestamp, item.timestamp);
    }
}
