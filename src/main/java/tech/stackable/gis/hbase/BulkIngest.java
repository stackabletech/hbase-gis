package tech.stackable.gis.hbase;

import ch.hsr.geohash.GeoHash;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.PrimitiveIterator;
import java.util.Random;

public class BulkIngest {

    private static final String usage = "ingest table source.tsv\n" +
            "  help - print this message and exit.\n" +
            "  table - the target table to load.\n" +
            "  family - the column family to use.\n" +
            "  count - the number of rows to generate and load.\n" +
            "  limits - the applied co-ordinate ranage in the form: lon_min,lon_max,lat_min,lat_max\n" +
            "\n" +
            "generates a geohash for the rowkey.\n" +
            "records are stored in columns in the specified family, columns are:\n" +
            "  lon,lat,id,name,address,city,url,phone,type,zip\n";

    public static void main(String[] args) throws IOException {

        if (args.length != 4) {
            System.out.println(usage);
            System.exit(0);
        }

        final TableName tableName = TableName.valueOf(args[0]);
        final Configuration conf = HBaseConfiguration.create();

        HBaseAdmin.available(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(tableName);

        final Admin admin = connection.getAdmin();
        byte[] family = String.valueOf(args[1]).getBytes();
        int row_count = Integer.parseInt(args[2]);
        long start = System.currentTimeMillis();
        String[] limits = String.valueOf(args[3]).split(",");

        // we need all 4
        if (limits.length != 4) {
            System.out.println(usage);
            System.exit(0);
        }

        // generate random co-ordinates within a fixed region
        PrimitiveIterator.OfDouble lon_iter = new Random().doubles(Double.valueOf(limits[0]), Double.valueOf(limits[1])).iterator();
        PrimitiveIterator.OfDouble lat_iter = new Random().doubles(Double.valueOf(limits[2]), Double.valueOf(limits[3])).iterator();

        try {
            for (int i = 0; i < row_count; i++) {
                Put put = getPut(family, i, lon_iter, lat_iter);
                table.put(put);
            }

            admin.flush(tableName);
        } finally {
            table.close();
            connection.close();
            admin.close();
        }

        long end = System.currentTimeMillis();
        System.out.printf("Geohashed %s records in %sms.%n", row_count, end - start);
    }

    public static Put getPut(byte[] family, int id, PrimitiveIterator.OfDouble lon_iter, PrimitiveIterator.OfDouble lat_iter) {
        double lon = lon_iter.nextDouble();
        double lat = lat_iter.nextDouble();
        int randomTextLength = 10;

        String rowkey = GeoHash.withCharacterPrecision(lat, lon, 12).toBase32();
        Put put = new Put(rowkey.getBytes());
        put.setDurability(Durability.SKIP_WAL);

        put.addColumn(family, "id".getBytes(), Integer.toString(id).getBytes());
        put.addColumn(family, "lon".getBytes(), Double.toString(lon).getBytes());
        put.addColumn(family, "lat".getBytes(), Double.toString(lat).getBytes());
        put.addColumn(family, "name".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
        put.addColumn(family, "address".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
        put.addColumn(family, "city".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
        put.addColumn(family, "url".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
        put.addColumn(family, "phone".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
        put.addColumn(family, "type".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
        put.addColumn(family, "zip".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
        return put;
    }
}
