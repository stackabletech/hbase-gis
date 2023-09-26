package HBaseIA.GIS.filter;

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

import java.nio.charset.StandardCharsets;
import java.util.List;


public class WithinFilter extends FilterBase {

    static final byte[] TABLE = "wifi".getBytes();
    static final byte[] FAMILY = "a".getBytes();
    static final byte[] ID = "id".getBytes();
    static final byte[] X_COL = "lon".getBytes();
    static final byte[] Y_COL = "lat".getBytes();

    static final Log LOG = LogFactory.getLog(WithinFilter.class);

    static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    protected Geometry query = null;
    protected boolean exclude = false;

    public WithinFilter(final Geometry query) {
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

        for (Cell cell : kvs) {
            if (CellUtil.matchingColumn(cell, FAMILY, X_COL))
                lon = parseCoordinate(cell);
            if (CellUtil.matchingColumn(cell, FAMILY, Y_COL))
                lat = parseCoordinate(cell);
        }

        if (Double.isNaN(lat) || Double.isNaN(lon)) {
            LOG.debug("Row is not a point.");
            this.exclude = true;
            return;
        }

        Coordinate coord = new Coordinate(lon, lat);
        Geometry point = GEOMETRY_FACTORY.createPoint(coord);
        //LOG.debug(String.format("query=%s, point=[%f, %f]", query.toString(), lat, lon));
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

    public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
        String query = new String(pbBytes, StandardCharsets.UTF_8);
        LOG.debug(String.format("parseFrom(%s)", query));
        WKTReader reader = new WKTReader(GEOMETRY_FACTORY);
        try {
            return new WithinFilter(reader.read(query));
        } catch (ParseException e) {
            throw new DeserializationException(e);
        }
    }

    /**
     * Returns The serialized filter
     */
    @Override
    public byte[] toByteArray() {
        final WKTWriter writer = new WKTWriter(2);
        return writer.write(this.query).getBytes(StandardCharsets.UTF_8);
    }

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
}
