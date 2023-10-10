package tech.stackable.gis.hbase;

import ch.hsr.geohash.GeoHash;
import com.google.common.base.Splitter;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Ingest {

    private static final String usage = "ingest table source.tsv\n" +
            "  help - print this message and exit.\n" +
            "  table - the target table to load.\n" +
            "  source.tsv - path to the tsv file to load.\n" +
            "\n" +
            "load data from source.tsv. assumes new-line delimited, tab-separated\n" +
            "records. drops the first line. generates a geohash for the rowkey.\n" +
            "records are stored in columns in the 'a' familt, columns are:\n" +
            "  lon,lat,id,name,address,city,url,phone,type,zip\n";

    private static final byte[] FAMILY = "a".getBytes();
    private static final String[] COLUMNS = new String[]{
            "lon", "lat", "id", "name", "address",
            "city", "url", "phone", "type", "zip"
    };
    private static final ArrayIterator COLS = new ArrayIterator(COLUMNS);
    private static final Splitter SPLITTER = Splitter.on('\t')
            .trimResults()
            .limit(COLUMNS.length);

    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            System.out.println(usage);
            System.exit(0);
        }

        final TableName tableName = TableName.valueOf(args[0]);

        final Configuration conf = HBaseConfiguration.create();

        HBaseAdmin.available(conf);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(tableName);

        final Admin admin = connection.getAdmin();

        BufferedReader reader = new BufferedReader(new FileReader(args[1]));
        String line = reader.readLine();
        int records = 0;
        long start = System.currentTimeMillis();

        try {
            while ((line = reader.readLine()) != null) {
                COLS.reset();
                Iterator<String> vals = SPLITTER.split(line).iterator();
                Map<String, String> row = new HashMap<>(COLUMNS.length);

                while (vals.hasNext() && COLS.hasNext()) {
                    String col = (String) COLS.next();
                    String val = vals.next();
                    row.put(col, val);
                }

                double lat = Double.parseDouble(row.get("lat"));
                double lon = Double.parseDouble(row.get("lon"));
                String rowkey = GeoHash.withCharacterPrecision(lat, lon, 12).toBase32();
                Put put = new Put(rowkey.getBytes());
                for (Map.Entry<String, String> e : row.entrySet()) {
                    put.addColumn(FAMILY, e.getKey().getBytes(), e.getValue().getBytes());
                }

                table.put(put);
                records++;
            }

            admin.flush(tableName);
        } finally {
            table.close();
            connection.close();
            reader.close();
            admin.close();
        }

        long end = System.currentTimeMillis();
        System.out.printf("Geohashed %s records in %sms.%n",
                records, end - start);

    }
}
