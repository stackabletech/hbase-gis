package tech.stackable.gis.hbase.filter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import tech.stackable.gis.hbase.shaded.protobuf.generated.FilterProtos;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class WithinFilter extends FilterBase {

    static final Log LOG = LogFactory.getLog(WithinFilter.class);
    static private final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private final Geometry query;
    private final byte[] table;
    private final byte[] family;
    private final byte[] lat_col;
    private final byte[] lon_col;
    protected boolean exclude = false;

    public WithinFilter(final Geometry query, byte[] table, byte[] family, byte[] latCol, byte[] lonCol) {
        this.query = query;
        this.table = table;
        this.family = family;
        this.lat_col = latCol;
        this.lon_col = lonCol;
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

        for (Cell cell : kvs) {
            if (CellUtil.matchingColumn(cell, family, lon_col))
                lon = parseCoordinate(cell);
            if (CellUtil.matchingColumn(cell, family, lat_col))
                lat = parseCoordinate(cell);
        }

        if (Double.isNaN(lat) || Double.isNaN(lon)) {
            LOG.debug("Row is not a point.");
            this.exclude = true;
            return;
        }

        final Coordinate coord = new Coordinate(lon, lat);
        Geometry point = GEOMETRY_FACTORY.createPoint(coord);
        this.exclude = !query.contains(point);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("row key=%s, lat=%f, lon=%f, filter applied=%s", rowKey(kvs.get(0)),
                    lat, lon,
                    (this.exclude ? "rejecting" : "keeping")));
    }

    @Override
    public boolean filterRow() {
        return this.exclude;
    }

    @Override
    public void reset() {
        this.exclude = false;
    }

    /**
     * Called by the region server when instantiating a new object.
     *
     * @param pbBytes A byte array as produced by {@link WithinFilter#toByteArray() toByteArray}
     * @return A new instance with the given query.
     * @throws DeserializationException
     */
    public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
        try {
            final FilterProtos.WithinFilter proto = FilterProtos.WithinFilter.parseFrom(pbBytes);
            final WKTReader reader = new WKTReader(GEOMETRY_FACTORY);
            return new WithinFilter(reader.read(proto.getQuery().toStringUtf8()),
                    proto.getTable().toByteArray(), proto.getFamily().toByteArray(), proto.getLatCol().toByteArray(), proto.getLonCol().toByteArray());
        } catch (InvalidProtocolBufferException | ParseException e) {
            throw new DeserializationException(e);
        }
    }

    /**
     * Called by the client when performing a filtered scan.
     *
     * @return A serialized query as expected by {@link WithinFilter#parseFrom(byte[]) parseFrom}
     */
    @Override
    public byte[] toByteArray() {
        final WKTWriter writer = new WKTWriter(2);
        final FilterProtos.WithinFilter.Builder builder = FilterProtos.WithinFilter.newBuilder();
        try {
            return builder.setQuery(ByteString.copyFrom(writer.write(this.query), "utf8"))
                    .setTable(ByteString.copyFrom(table))
                    .setFamily(ByteString.copyFrom(family))
                    .setLatCol(ByteString.copyFrom(lat_col))
                    .setLonCol(ByteString.copyFrom(lon_col))
                    .build().toByteArray();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parse a coordinate cell's value from it's String representation to a Double.
     * It's the caller's responsibility to provide the correct cell.
     *
     * @param cell A coordinate cell.
     * @return A coordinate value or Double.NaN in case if an error.
     */
    final Double parseCoordinate(final Cell cell) {
        byte[] value = null;
        int offset = 0;
        int len = 0;
        if (cell instanceof ByteBufferExtendedCell) {
            value = ((ByteBufferExtendedCell) cell).getValueByteBuffer().array();
            offset = ((ByteBufferExtendedCell) cell).getValuePosition();
            len = cell.getValueLength();
        } else {
            value = cell.getValueArray();
            offset = cell.getValueOffset();
            len = cell.getValueLength();
        }
        try {
            return Double.parseDouble(Bytes.toString(value, offset, len));
        } catch (NumberFormatException nfe) {
            LOG.error(String.format("Failed to parse coordinate for key " + CellUtil.getCellKeyAsString(cell)));
        }
        return Double.NaN;
    }

    final String rowKey(final Cell c) {
        return Bytes.toString(c.getRowArray(), c.getRowOffset(), c.getRowLength());
    }
}
