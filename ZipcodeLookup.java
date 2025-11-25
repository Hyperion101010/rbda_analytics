import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.locationtech.jts.geom.Coordinate;
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

        ZipShape(String zipcode, Geometry geometry) {
            this.zipcode = zipcode;
            this.geometry = geometry;
        }
    }

    private final List<ZipShape> shapes = new ArrayList<>();
    private final GeometryFactory geometryFactory = new GeometryFactory();

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
                inputStream = fs.open(path);
            } else {
                java.io.File localFile = new java.io.File(csvPath);
                if (localFile.exists()) {
                    inputStream = new java.io.FileInputStream(localFile);
                } else {
                    throw new IOException("Zipcode CSV file not found: " + csvPath);
                }
            }

            BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .parse(reader);

            WKTReader wktReader = new WKTReader(geometryFactory);

            for (CSVRecord record : records) {
                String modZcta = record.get("MODZCTA");    // e.g. "10001"
                String wkt = record.get("the_geom");       // MULTIPOLYGON(...)

                if (modZcta == null || modZcta.isEmpty() ||
                    wkt == null || wkt.isEmpty()) {
                    continue; // skip bad rows
                }

                try {
                    Geometry geom = wktReader.read(wkt);
                    shapes.add(new ZipShape(modZcta, geom));
                } catch (Exception e) {
                    // If one row is bad, skip it but don't kill the whole job
                    System.err.println("Failed to parse WKT for MODZCTA "
                            + modZcta + ": " + e.getMessage());
                }
            }
            
            // Note: Using simple linear search to match Python implementation exactly
            // Python code: for zip_code, poly in zip_polygons: if poly.contains(point): return zip_code
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }

    /**
     * Empty constructor for fallback
     */
    public ZipcodeLookup() {
        // shapes is already initialized as empty ArrayList
    }

    /**
     * Look up zipcode for a given latitude and longitude.
     * Matches Python implementation: simple linear search through all polygons.
     * Python: for zip_code, poly in zip_polygons: if poly.contains(point): return zip_code
     * NOTE: WKT is in (lon, lat), so we must create Point(lon, lat).
     * @param latitude latitude in decimal degrees (WGS84)
     * @param longitude longitude in decimal degrees (WGS84)
     * @return zipcode string if found, null otherwise
     */
    public String findZipcode(double latitude, double longitude) {
        // Create Point(lon, lat) - matching Python: Point(lon, lat)
        Point p = geometryFactory.createPoint(new Coordinate(longitude, latitude));

        // Simple linear search through all polygons (matching Python exactly)
        for (ZipShape shape : shapes) {
            if (shape.geometry.contains(p)) {
                return shape.zipcode;
            }
        }

        // Not found (matching Python: returns None)
        return null;
    }
}

