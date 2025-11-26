import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;


// This is my CSV helper class
class csv_col_schema {
    public static final LinkedHashMap<String, Integer> nypd_csv_cols = new LinkedHashMap<String, Integer>() {{
    // Insert all the correct columns in correct order for my third party CSV parser
    put("ARREST_KEY", 0);
    put("ARREST_DATE", 1);
    put("PD_CD", 2);
    put("PD_DESC", 3);
    put("KY_CD", 4);
    put("OFNS_DESC", 5);
    put("LAW_CODE", 6);
    put("LAW_CAT_CD", 7);
    put("ARREST_BORO", 8);
    put("ARREST_PRECINCT", 9);
    put("JURISDICTION_CODE", 10);
    put("AGE_GROUP", 11);
    put("PERP_SEX", 12);
    put("PERP_RACE", 13);
    put("X_COORD_CD", 14);
    put("Y_COORD_CD", 15);
    put("Latitude", 16);
    put("Longitude", 17);
    put("Lon_Lat", 18);
    }};

    public static final int col_count = nypd_csv_cols.size();
}


// This is my zipcode utility class

// JTS suite for vector geometry operations
// reference - https://github.com/locationtech/jts
// a small tutorial - https://webmonkeyswithlaserbeams.wordpress.com/2008/08/26/java-topology-suite-sweet/
// I initially tried this approach using shapely in python then I implemeted it in java using JTS
// my python refereence article - https://www.geopostcodes.com/blog/get-zip-code-from-latitude-and-longitude/
class zip_code_util {
    private static class zip_code_shape_data_type {
        final String zipcode;
        final Geometry geometry;

        zip_code_shape_data_type(String zipcode, Geometry geometry) {
            this.zipcode = zipcode;
            this.geometry = geometry;
        }
    }

    private final List<zip_code_shape_data_type> zip_code_shapes_lst = new ArrayList<>();
    private final GeometryFactory geometry_factory = new GeometryFactory();

    public zip_code_util(String zipcode_shape_file_path, Configuration conf) throws IOException {

        // Get the zipcode lookup file.
        // reference - https://data.cityofnewyork.us/Health/Modified-Zip-Code-Tabulation-Areas-MODZCTA-/pri4-ifjk/about_data
        Path path = new Path(zipcode_shape_file_path);
        FileSystem fs = path.getFileSystem(conf);
        InputStream in_stream = null;
        try {
            if (fs.exists(path)) {
                in_stream = fs.open(path);
            } else {
                java.io.File localFile = new java.io.File(zipcode_shape_file_path);
                if (localFile.exists()) {
                    in_stream = new java.io.FileInputStream(localFile);
                } else {
                    throw new IOException("Zipcode CSV file not found: " + zipcode_shape_file_path);
                }
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(in_stream, StandardCharsets.UTF_8));
            // Open the zipcode lookup file provided from the above reference link
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(br);

            // Needed for parsing the geometry of the zipcode shapes
            // reference - https://locationtech.github.io/jts/javadoc/org/locationtech/jts/io/WKTReader.html
            WKTReader wkt_reader = new WKTReader();

            for(CSVRecord record : records) {
                String each_zipcode = record.get("MODZCTA");
                String each_wkt_shape = record.get("the_geom");

                if(each_zipcode == null || each_wkt_shape == null) {
                    continue;
                }

                try {
                    Geometry g = wkt_reader.read(each_wkt_shape);
                    zip_code_shapes_lst.add(new zip_code_shape_data_type(each_zipcode, g));
                } catch (Exception e) {
                    continue;
                }
            }
        } finally {
            if (in_stream != null) {
                in_stream.close();
            }
        }
    }

    public String get_zipcode_from_lat_long(double latitude, double longitude) {
        Point pt = geometry_factory.createPoint(new Coordinate(longitude, latitude));

        for (zip_code_shape_data_type zip_shape_data : zip_code_shapes_lst) {
            if (zip_shape_data.geometry.contains(pt)) {
                return zip_shape_data.zipcode;
            }
        }
        return null;
    }
}

public class NYPDArrestsDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    // reference - https://data.cityofnewyork.us/Public-Safety/NYPD-Arrests-Data-Historic-/8h9b-rp9u/about_data
    private static final LinkedHashMap<String, String> dataset_schema = new LinkedHashMap<String, String>() {{
        put("ARREST_KEY", "TEXT");
        put("ARREST_DATE", "TIMESTAMP");
        put("PD_CD", "NUMBER");
        put("PD_DESC", "TEXT");
        put("KY_CD", "NUMBER");
        put("OFNS_DESC", "TEXT");
        put("LAW_CODE", "TEXT");
        put("LAW_CAT_CD", "TEXT");
        put("ARREST_BORO", "TEXT");
        put("ARREST_PRECINCT", "NUMBER");
        put("JURISDICTION_CODE", "NUMBER");
        put("AGE_GROUP", "TEXT");
        put("PERP_SEX", "TEXT");
        put("PERP_RACE", "TEXT");
        put("X_COORD_CD", "TEXT");
        put("Y_COORD_CD", "TEXT");
        put("Latitude", "NUMBER");
        put("Longitude", "NUMBER");
        put("Lon_Lat", "POINT");
    }};

    // These fields won't be used by me so I am dropping them.
    private static final ArrayList<String> col_to_drop = new ArrayList<String>() {{
        add("X_COORD_CD");
        add("Y_COORD_CD");
        add("PERP_RACE");
        add("Lon_Lat");
    }};

    public static final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm");
    public static final DateTimeFormatter output_date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final LocalDateTime threshold_date = LocalDateTime.of(2015, 1, 1, 0, 0, 0);

    // Defined in the dataset page
    // reference - https://data.cityofnewyork.us/Public-Safety/NYPD-Arrests-Data-Historic-/8h9b-rp9u/about_data
    private static final Map<String, String> keyword_to_boro_map = new LinkedHashMap<String, String>() {{
        put("K", "Brooklyn");
        put("Q", "Queens");
        put("M", "Manhattan");
        put("B", "Bronx");
        put("S", "Staten Island");
    }};

    // Used to map law category code to description like misdemeanor, felony etc.
    private static final Map<String, String> law_ct_mapping = new LinkedHashMap<String, String>() {{
        put("M", "Misdemeanor");
        put("F", "Felony");
        put("V", "Violation");
    }};

    private static final ArrayList<String> final_op_cols = new ArrayList<String>() {{
        add("ARREST_KEY");
        add("ARREST_DATE");
        add("OFNS_DESC");
        add("PD_CD");
        add("KY_CD");
        add("LAW_CAT_CD");
        add("ARREST_BORO");
        add("ARREST_PRECINCT");
        add("AGE_GROUP");
        add("AGE_MIN");
        add("AGE_MAX");
        add("PERP_SEX");
        add("JURISDICTION_CODE");
        add("PD_DESC");
        add("LAW_CODE");
        add("ZIPCODE");
    }};

    private zip_code_util zipcode_util_obj;
    private CSVParser csv_parser;

    // Load the zipcode lookup file in my setup for each mapper.
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        // reference - https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVParser.html
        // need the quote char to be double as some of my entries in csv have double quotes
        this.csv_parser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').withEscapeChar('\\').build();

        Configuration conf = context.getConfiguration();
        String my_zip_code_file = conf.get("zipcode.lookup.file");

        this.zipcode_util_obj = new zip_code_util(my_zip_code_file, conf);
    }


    // Actual mapper function, this is where I am doing most of my preprocessing of getting the data and cleaning it.
    // I am dropping some colums from here.
    // I am also taking only those rows above a specific time threshold.
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parsed_col_data;

        String each_line = value.toString();

        // Stats to count total rows processed
        context.getCounter("STATS", "TOTAL_ROWS").increment(1);

        if(each_line.startsWith("ARREST_KEY")) {
            // no need to do anything with header row.
            return;
        }

        try{
            parsed_col_data = this.csv_parser.parseLine(each_line);
            if(parsed_col_data.length != csv_col_schema.col_count) {
                return;
            }
        } catch (Exception e) {
            return;
        }


        // Till this step now I have parsed the input properly.

        // Let's index the col name and value from our mappings.
        // This will help in easier lookups in validations steps.
        LinkedHashMap<String, String> row_field_name_to_val_map = new LinkedHashMap<>();
        Iterator<Map.Entry<String, Integer>> col_schema_itr = csv_col_schema.nypd_csv_cols.entrySet().iterator();

        while(col_schema_itr.hasNext()) {
            Map.Entry<String, Integer> each_col_entry = col_schema_itr.next();

            String each_col_name = each_col_entry.getKey();
            int each_col_idx = each_col_entry.getValue();

            String each_col_val = parsed_col_data[each_col_idx];

            row_field_name_to_val_map.put(each_col_name, each_col_val);
        }

        LinkedHashMap<String, String> validated_row_map = validate_and_transform_row_entry(row_field_name_to_val_map);

        if(validated_row_map == null) {
            return;
        }

        if(validated_row_map.get("ARREST_DATE") == null) {
            return;
        }
        else{
            LocalDateTime arrest_date = LocalDateTime.parse(validated_row_map.get("ARREST_DATE"), output_date_formatter);

            // Check if date is above threshold.
            // If not then drop this row.
            if(arrest_date.isBefore(threshold_date)) {
                return;
            }
        }

        String lat = validated_row_map.get("Latitude");
        String lng = validated_row_map.get("Longitude");

        if(lat != null && lng != null && !lat.equals("0") && !lng.equals("0") && !lat.isEmpty() && !lng.isEmpty()) {
            double lt, lg;

            try {
                lt = Double.parseDouble(lat);
                lg = Double.parseDouble(lng);
            } catch (NumberFormatException e) {
                return;
            }

            String resolved_zipcode = this.zipcode_util_obj.get_zipcode_from_lat_long(lt, lg);

            if(!is_zipcode_valid(resolved_zipcode)) {
                return;
            }

            validated_row_map.put("ZIPCODE", resolved_zipcode);
        } else {
            // Drop the entry that was no valid zipcode resolution due to improper lat long values
            return;
        }

        validated_row_map.remove("Latitude");
        validated_row_map.remove("Longitude");



        StringBuilder csvLine = new StringBuilder();
        for (int i = 0; i < final_op_cols.size(); i++) {
            String col = final_op_cols.get(i);
            String val = validated_row_map.getOrDefault(col, "");

            // Handle escape characters for CSV
            if (val.contains(",") || val.contains("\"") || val.contains("\n")) {
                val = "\"" + val.replace("\"", "\"\"") + "\"";
            }
            
            if (i > 0) {
                csvLine.append(",");
            }
            csvLine.append(val);
        }

        context.getCounter("STATS", "ALLOWED_ROW").increment(1);

        // From the multiple outputs, write to data named output
        context.write(new Text("DATA:"), new Text(csvLine.toString()));
    }


    private LinkedHashMap<String, String> validate_and_transform_row_entry(LinkedHashMap<String, String> row_field_name_to_val_map) {
        LinkedHashMap<String, String> validated_map = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : row_field_name_to_val_map.entrySet()) {
            String col_name = entry.getKey();
            String col_value = entry.getValue();

            // We are not considering null entries.
            if(col_value == null || col_value.trim().isEmpty() || col_value.equalsIgnoreCase("NA") || col_value.equalsIgnoreCase("null")) {
                continue;
            }

            // We are also dropping columns that we don't need.
            if(col_to_drop.contains(col_name)) {
                continue;
            }


            // Now lets check each col one by one
            if(col_name.equals("ARREST_DATE")) {
                try{
                    LocalDateTime arrest_date;

                    if(col_value.length() == 10) {
                        arrest_date = LocalDateTime.parse(col_value + " 00:00:00", 
                            DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
                    } else {
                        arrest_date = LocalDateTime.parse(col_value, 
                            DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
                    }

                    String op_date = arrest_date.format(output_date_formatter);
                    validated_map.put("ARREST_DATE", op_date);
                } catch (DateTimeParseException e) {
                    continue;
                }
                continue;
            }

            if(col_name.equals("ARREST_BORO")) {
                String boro_full_name = keyword_to_boro_map.get(col_value.trim().toUpperCase());

                // Borough lookup failed
                if(boro_full_name == null) {
                    continue;
                }
                validated_map.put("ARREST_BORO", boro_full_name);
                continue;
            }

            if(col_name.equals("LAW_CAT_CD")) {
                String law_cat_full = law_ct_mapping.get(col_value.trim().toUpperCase());

                // Law category code lookup failed
                if(law_cat_full == null) {
                    continue;
                }
                validated_map.put("LAW_CAT_CD", law_cat_full);
                continue;
            }


            if(col_name.equals("AGE_GROUP")) {
                String resolved_age_group = resolve_age_group(col_value.trim());

                if(resolved_age_group == null) {
                    continue;
                }

                String limits[] = resolved_age_group.split("-");
                int min_age, max_age;
                if(limits.length == 2) {
                    try {
                        min_age = Integer.parseInt(limits[0].trim());
                        max_age = Integer.parseInt(limits[1].trim());
                    } catch (NumberFormatException e) {
                        continue;
                    }

                    validated_map.put("AGE_MIN", String.valueOf(min_age));
                    validated_map.put("AGE_MAX", String.valueOf(max_age));
                }
                validated_map.put("AGE_GROUP", resolved_age_group);
                continue;
            }

            if(col_name.equals("PD_CD") || col_name.equals("KY_CD") || col_name.equals("ARREST_PRECINCT") || col_name.equals("JURISDICTION_CODE")) {
                try {
                    double num_val = Double.parseDouble(col_value.trim());

                    validated_map.put(col_name, String.valueOf((int) num_val));
                } catch (NumberFormatException e) {
                    continue;
                }
                continue;
            }

            validated_map.put(col_name, col_value);
        }

        if(!validated_map.containsKey("ARREST_KEY") || !validated_map.containsKey("ARREST_DATE")) {
            return null;
        }

        return validated_map;
    }

    private String resolve_age_group(String agelimit) {
        if (agelimit == null || agelimit.isEmpty()) {
            return agelimit;
        }

        if (agelimit.equals("<18")) {
            return "0-17";
        }

        if (agelimit.equals("65+")) {
            return "65-100";
        }

        return agelimit.trim();
    }

    private boolean is_zipcode_valid(String zipcode) {
        if (zipcode == null || zipcode.trim().isEmpty()) {
            return false;
        }

        try {
            int zip = Integer.parseInt(zipcode.trim());

            // These are usual ranges for NYC zipcodes
            if(zip < 10000 || zip > 11699) {
                return false;
            }
            return true;
        } catch (NumberFormatException e) {
            // If zipcode is not an Int and can't be parsed, just drop this entry.
        }
        return false;
    }
}
