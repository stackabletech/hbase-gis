package tech.stackable.gis.hbase.filter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import tech.stackable.gis.hbase.shaded.protobuf.generated.FilterProtos;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Optional;

public class WithinFilter extends FilterBase {

    static final Log LOG = LogFactory.getLog(WithinFilter.class);
    static private final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private final Geometry query;
    private final byte[] family;
    private final byte[] lat_col;
    private final byte[] lon_col;
    // The longitude value parsed out of the column given by lon_col
    // This value is reset at the the beginning of each row scan.
    private Optional<Double> cell_lon = Optional.empty();
    // The latitude value parsed out of the column given by lan_col
    // This value is reset at the the beginning of each row scan.
    private Optional<Double> cell_lat = Optional.empty();

    public WithinFilter(final Geometry query, byte[] family, byte[] latCol, byte[] lonCol) {
        this.query = query;
        this.family = family;
        this.lat_col = latCol;
        this.lon_col = lonCol;
    }

    /**
     * Speed up scanning by only looking at the relevant column family.
     *
     * @param name Column family name that is about to be scanned
     * @return true if the name matches the {@link #family} field
     * @throws IOException
     */
    @Override
    public boolean isFamilyEssential(byte[] name) throws IOException {
        return Bytes.equals(name, this.family);
    }

    /**
     * Called at the beginning at every row scan.
     *
     * @throws IOException
     */
    @Override
    public void reset() throws IOException {
        cell_lat = cell_lon = Optional.empty();
    }

    /**
     * Called for each cell in the row but with shortcuts:
     * - stops scanning the row cells if a decision regarding the row can be made
     * - only scans cells of the "essential" family
     *
     * @param cell the Cell in question
     * @return See {@link org.apache.hadoop.hbase.filter.Filter.ReturnCode}
     * @throws IOException
     */
    @Override
    public ReturnCode filterCell(Cell cell) throws IOException {
        if (CellUtil.matchingColumn(cell, family, lon_col))
            cell_lon = Optional.of(parseCoordinate(cell));
        if (CellUtil.matchingColumn(cell, family, lat_col))
            cell_lat = Optional.of(parseCoordinate(cell));

        if (cell_lat.isPresent() && cell_lon.isPresent()) {
            if (Double.isNaN(cell_lat.get()) || Double.isNaN(cell_lon.get())) {
                LOG.debug("Either lat or lon are NaN");
                return ReturnCode.NEXT_ROW;
            }

            final Coordinate coord = new Coordinate(cell_lon.get(), cell_lat.get());
            final Geometry point = GEOMETRY_FACTORY.createPoint(coord);
            final boolean isWithin = !query.covers(point);
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("row key=%s, lat=%f, lon=%f, filter applied=%s", rowKey(cell),
                        cell_lat.get(), cell_lon.get(),
                        (isWithin ? "rejecting" : "keeping")));
            return isWithin ?
                    ReturnCode.SKIP : ReturnCode.INCLUDE;
        }
        return ReturnCode.NEXT_COL;
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
                    proto.getFamily().toByteArray(), proto.getLatCol().toByteArray(), proto.getLonCol().toByteArray());
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
        byte[] value;
        int offset;
        int len;
        if (cell instanceof ByteBufferExtendedCell) {
            value = ((ByteBufferExtendedCell) cell).getValueByteBuffer().array();
            offset = ((ByteBufferExtendedCell) cell).getValuePosition();
        } else {
            value = cell.getValueArray();
            offset = cell.getValueOffset();
        }
        len = cell.getValueLength();
        try {
            return Double.parseDouble(Bytes.toString(value, offset, len));
        } catch (NumberFormatException nfe) {
            LOG.error("Failed to parse coordinate for key " + CellUtil.getCellKeyAsString(cell));
        }
        return Double.NaN;
    }

    final String rowKey(final Cell c) {
        return Bytes.toString(c.getRowArray(), c.getRowOffset(), c.getRowLength());
    }
}
