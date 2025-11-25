import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Point-in-polygon zipcode lookup using JTS geometry.
 * Loads zipcode polygons from CSV file with WKT MULTIPOLYGON geometries.
 */
public class ZipcodeLookup {

    private static class ZipShape {
        final String zipcode;
        final Geometry geometry;
        final Envelope envelope;

        ZipShape(String zipcode, Geometry geometry) {
            this.zipcode = zipcode;
            this.geometry = geometry;
            this.envelope = geometry.getEnvelopeInternal();
        }
    }

    private final List<ZipShape> shapes = new ArrayList<>();
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private int loadedPolygonCount = 0;
    private String headersInfo = null;
    private String fileSource = null;

    /**
     * Load all zipcode polygons from the CSV file.
     * Supports both HDFS and local filesystem paths.
     * @param csvPath path to nyc_zip_data_lookup.csv (HDFS or local)
     * @param conf Hadoop Configuration
     */
    public ZipcodeLookup(String csvPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(csvPath);

        InputStream inputStream = null;
        try {
            if (fs.exists(path)) {
                this.fileSource = "HDFS: " + path.toString();
                inputStream = fs.open(path);
            } else {
                java.io.File localFile = new java.io.File(csvPath);
                if (localFile.exists()) {
                    this.fileSource = "LOCAL: " + localFile.getAbsolutePath();
                    inputStream = new java.io.FileInputStream(localFile);
                } else {
                    throw new IOException("Zipcode CSV file not found in HDFS or local FS: " + csvPath);
                }
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            // Parse CSV with header row
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .parse(reader);

            // Debug: check header names once
            if (records.iterator().hasNext()) {
                CSVRecord first = records.iterator().next();
                this.headersInfo = "Headers: " + first.toMap().keySet().toString();
                // Reset reader since we advanced the iterator
                reader.close();
                if (fs.exists(path)) {
                    inputStream = fs.open(path);
                } else {
                    inputStream = new java.io.FileInputStream(csvPath);
                }
                reader = new BufferedReader(
                        new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
            }

            WKTReader wktReader = new WKTReader(geometryFactory);

            int loadedCount = 0;
            for (CSVRecord record : records) {
                String modZcta;
                String wkt;

                try {
                    // IMPORTANT: column names must match your CSV exactly
                    modZcta = record.get("MODZCTA");     // e.g. "10001"
                    wkt = record.get("the_geom");        // MULTIPOLYGON(...)
                } catch (IllegalArgumentException e) {
                    // This means the headers don't match what we expect
                    throw new IOException(
                        "ZipcodeLookup: CSV does not contain expected columns 'MODZCTA' and 'the_geom': "
                        + e.getMessage(), e);
                }

                if (modZcta == null || modZcta.isEmpty() ||
                    wkt == null || wkt.isEmpty()) {
                    continue; // skip bad rows
                }

                try {
                    Geometry geom = wktReader.read(wkt);

                    // Ensure geometry is valid (fixes self-intersections, etc.)
                    if (!geom.isValid()) {
                        geom = geom.buffer(0);
                    }

                    shapes.add(new ZipShape(modZcta, geom));
                    loadedCount++;
                } catch (Exception e) {
                    // Log parse failures via counter if available
                    // Note: Individual WKT parse failures are not critical, so we continue
                }
            }

            // Store diagnostic info: we'll expose this via a getter and set counter in mapper
            this.loadedPolygonCount = loadedCount;

        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    /**
     * Empty constructor for fallback (no shapes).
     */
    public ZipcodeLookup() {
        // shapes is already initialized as empty ArrayList
        this.loadedPolygonCount = 0;
    }

    /**
     * Get diagnostic information for counter logging.
     */
    public int getLoadedPolygonCount() {
        return loadedPolygonCount;
    }

    public String getHeadersInfo() {
        return headersInfo;
    }

    public String getFileSource() {
        return fileSource;
    }

    /**
     * Look up zipcode for a given latitude and longitude.
     * WKT and JTS use (x, y) = (lon, lat), so we must create Point(lon, lat).
     * @param latitude latitude in decimal degrees (WGS84)
     * @param longitude longitude in decimal degrees (WGS84)
     * @return zipcode string if found, null otherwise
     */
    public String findZipcode(double latitude, double longitude) {
        // If we have no polygons, bail out fast
        if (shapes.isEmpty()) {
            return null;
        }

        // Create Point(lon, lat)
        Point p = geometryFactory.createPoint(new Coordinate(longitude, latitude));
        if (!p.isValid()) {
            return null;
        }

        // Simple linear search with envelope pre-check, then covers()
        for (ZipShape shape : shapes) {
            // Quick bounding-box filter
            if (!shape.envelope.contains(p.getCoordinate())) {
                continue;
            }

            Geometry geom = shape.geometry;

            // Use covers() instead of contains() so that boundary points are also matched
            if (geom.covers(p)) {
                return shape.zipcode;
            }
        }

        // Not found
        return null;
    }
}
