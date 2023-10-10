package tech.stackable.gis.hbase.coprocessor;

public class Item implements Comparable<Item> {
    public final String key;
    public final String item;
    public final long timestamp;

    public Item(String key, String item, long timestamp) {
        this.key = key;
        this.item = item;
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(Item item) {
        return Long.compare(this.timestamp, item.timestamp);
    }
}
