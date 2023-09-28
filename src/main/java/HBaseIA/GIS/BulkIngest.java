package HBaseIA.GIS;

import ch.hsr.geohash.GeoHash;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

public class BulkIngest {

  private static final String usage = "ingest table source.tsv\n" +
      "  help - print this message and exit.\n" +
      "  table - the target table to load.\n" +
      "  count - the number of rows to generate and load.\n" +
      "\n" +
      "generates a geohash for the rowkey.\n" +
      "records are stored in columns in the 'a' family, columns are:\n" +
      "  lon,lat,id,name,address,city,url,phone,type,zip\n";

  private static final byte[] FAMILY = "a".getBytes();

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

    int row_count = Integer.parseInt(args[1]);
    long start = System.currentTimeMillis();

    // generate random co-ordinates within a fixed region
    PrimitiveIterator.OfDouble lon_iter = new Random().doubles(-75, -70).iterator();
    PrimitiveIterator.OfDouble lat_iter = new Random().doubles(40, 45).iterator();

    try {
      for (int i = 0; i < row_count; i++) {
        Put put = getPut(i, lon_iter, lat_iter);
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

  public static Put getPut(int id, PrimitiveIterator.OfDouble lon_iter, PrimitiveIterator.OfDouble lat_iter) {
    double lon = lon_iter.nextDouble();
    double lat = lat_iter.nextDouble();
    int randomTextLength= 10;

    String rowkey = GeoHash.withCharacterPrecision(lat, lon, 12).toBase32();
    Put put = new Put(rowkey.getBytes());

    put.addColumn(FAMILY, "id".getBytes(), Integer.toString(id).getBytes());
    put.addColumn(FAMILY, "lon".getBytes(), Double.toString(lon).getBytes());
    put.addColumn(FAMILY, "lat".getBytes(), Double.toString(lat).getBytes());
    put.addColumn(FAMILY, "name".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
    put.addColumn(FAMILY, "address".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
    put.addColumn(FAMILY, "city".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
    put.addColumn(FAMILY, "url".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
    put.addColumn(FAMILY, "phone".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
    put.addColumn(FAMILY, "type".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
    put.addColumn(FAMILY, "zip".getBytes(), RandomStringUtils.randomAlphabetic(randomTextLength).getBytes());
    return put;
  }
}
