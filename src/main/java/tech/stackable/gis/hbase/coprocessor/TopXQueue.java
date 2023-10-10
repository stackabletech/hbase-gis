package tech.stackable.gis.hbase.coprocessor;

import java.util.PriorityQueue;

public class TopXQueue extends PriorityQueue<Item> {
    private static final long serialVersionUID = 5134028249611535803L;
    private final int count;

    public TopXQueue(int count) {
        super(count);
        this.count = count;
    }

    @Override
    public boolean add(Item item) {
        super.add(item);
        if (size() > count) {
            poll(); // remove smallest i.e. oldest value
        }
        return true;
    }
}
