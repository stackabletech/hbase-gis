package HBaseIA.GIS.filter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

 
public class WithinFilter extends FilterBase {

  static final byte[] TABLE = "wifi".getBytes();
  static final byte[] FAMILY = "a".getBytes();
  static final byte[] ID = "id".getBytes();
  static final byte[] X_COL = "lon".getBytes();
  static final byte[] Y_COL = "lat".getBytes();

  static final Log LOG = LogFactory.getLog(WithinFilter.class);

  static final GeometryFactory factory = new GeometryFactory();
  Geometry query = null;
  boolean exclude = false;

  public WithinFilter(Geometry query) {
    this.query = query;
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public void filterRowCells(List<Cell> kvs) {
    double lon = Double.NaN;
    double lat = Double.NaN;

    if (null == kvs || 0 == kvs.size()) {
      LOG.debug("skipping empty row.");
      this.exclude = true;
      return;
    }

    for (Cell kv : kvs) {
      byte[] qual = kv.getQualifierArray();
      if (Bytes.equals(qual, X_COL))
        lon = Double.parseDouble(new String(kv.getValueArray()));
      if (Bytes.equals(qual, Y_COL))
        lat = Double.parseDouble(new String(kv.getValueArray()));
    }

    if (Double.isNaN(lat) || Double.isNaN(lon)) {
      // TODO: how to get the key from a Cell ???
      // LOG.debug(kvs.get(0).getKeyString() + " is not a point.");
      LOG.debug("is not a point.");
      this.exclude = true;
      return;
    }

    Coordinate coord = new Coordinate(lon, lat);
    Geometry point = factory.createPoint(coord);
    if (!query.contains(point)) {
      this.exclude = true;
    }
  }

  @Override
  public boolean filterRow() {
    if (LOG.isDebugEnabled())
      LOG.debug("filter applied. " + (this.exclude ? "rejecting" : "keeping"));
    return this.exclude;
  }

  @Override
  public void reset() {
    this.exclude = false;
  }

  /** TODO: serialize filter */
  /* 
  @Override
  public byte[] toByteArray() {
    Built-in HBase filters use Protobuf for serialization.
    This is probably required since there is no API for deserialization.
    out.writeUTF(query.toText());
  }
  */

  public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
    String wkt = new String(pbBytes, StandardCharsets.UTF_8);
    WKTReader reader = new WKTReader(factory);
    try {
      return new WithinFilter(reader.read(wkt));
    } catch (ParseException e) {
      throw new DeserializationException(e);
    }
  }

  /*
  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    String wkt = new String(filterArguments.get(0), StandardCharsets.UTF_8);
    WKTReader reader = new WKTReader(factory);
    try {
      return new WithinFilter(reader.read(wkt));
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }
   */
}
