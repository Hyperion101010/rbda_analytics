import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class NYPDArrestsDataMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final Map<String, String> SCHEMA = Map.ofEntries(
        Map.entry("ARREST_KEY", "TEXT"),
        Map.entry("ARREST_DATE", "TIMESTAMP"),
        Map.entry("PD_CD", "NUMBER"),
        Map.entry("PD_DESC", "TEXT"),
        Map.entry("KY_CD", "NUMBER"),
        Map.entry("OFNS_DESC", "TEXT"),
        Map.entry("LAW_CODE", "TEXT"),
        Map.entry("LAW_CAT_CD", "TEXT"),
        Map.entry("ARREST_BORO", "TEXT"),
        Map.entry("ARREST_PRECINCT", "NUMBER"),
        Map.entry("JURISDICTION_CODE", "NUMBER"),
        Map.entry("AGE_GROUP", "TEXT"),
        Map.entry("PERP_SEX", "TEXT"),
        Map.entry("PERP_RACE", "TEXT"),
        Map.entry("X_COORD_CD", "TEXT"),
        Map.entry("Y_COORD_CD", "TEXT"),
        Map.entry("Latitude", "NUMBER"),
        Map.entry("Longitude", "NUMBER"),
        Map.entry("Lon_Lat", "POINT")
    );

    private static final Set<String> COLUMNS_TO_DROP = Set.of(
        "X_COORD_CD", "Y_COORD_CD", "PERP_RACE", "Lon_Lat"
    );

    private static final DateTimeFormatter INPUT_DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    private static final DateTimeFormatter OUTPUT_DATE_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss MM/dd/yyyy");
    private static final LocalDateTime MIN_DATE = LocalDateTime.of(2015, 1, 1, 0, 0, 0);

    private static final Map<String, String> BORO_MAPPING = Map.of(
        "K", "Brooklyn",
        "Q", "Queens",
        "M", "Manhattan",
        "B", "Bronx",
        "S", "Staten Island"
    );

    private static final Map<String, String> LAW_CAT_MAPPING = Map.of(
        "M", "Misdemeanor",
        "F", "Felony",
        "V", "Violation"
    );


    private static final String[] OUTPUT_COLUMNS = new String[] {
        "ARREST_KEY",
        "date",
        "OFNS_DESC",
        "PD_CD",
        "KY_CD",
        "Category_of_offense",
        "borough",
        "ARREST_PRECINCT",
        "AGE_GROUP",
        "AGE_MIN",
        "AGE_MAX",
        "PERP_SEX",
        "JURISDICTION_CODE",
        "ZIP_CODE",
        "PD_DESC",
        "LAW_CODE",
        "LAW_CAT_CD"
    };

    // Reference to column index mapping
    private static final Map<String, Integer> COL = CsvSchema.COL;
    
    private CSVParser csvParser;
    private ZipCodeLookup zipCodeLookup;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        csvParser = new CSVParserBuilder()
                .withSeparator(',')   // standard CSV
                .withQuoteChar('"')   // handle "quoted, fields"
                .withEscapeChar('\\') // allow \" inside
                .build();

        Configuration conf = context.getConfiguration();
        String zipcodeFile = conf.get("zipcode.bounds.file", "nyc_zip_data_lookup.csv");
        
        try {
            zipCodeLookup = new ZipCodeLookup(zipcodeFile, conf);
        } catch (Exception e) {
            System.err.println("Warning: Could not load zipcode CSV file: " + e.getMessage());
            zipCodeLookup = new ZipCodeLookup();
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        if (line.isEmpty()) {
            return;
        }

        if (key.get() == 0 && line.startsWith("ARREST_KEY,")) {
            return;
        }

        String[] fields;
        try {
            fields = csvParser.parseLine(line);
        } catch (Exception e) {
            return;
        }

        // Safety check: ensure expected number of columns
        if (fields.length < CsvSchema.EXPECTED_COLUMN_COUNT) {
            return;
        }

        // Build record map using column index mapping
        Map<String, String> record = new HashMap<>();
        for (Map.Entry<String, Integer> entry : COL.entrySet()) {
            String colName = entry.getKey();
            int colIndex = entry.getValue();
            if (colIndex < fields.length) {
                String fieldValue = fields[colIndex].trim();
                record.put(colName, fieldValue);
            }
        }

        Map<String, String> cleanedRecord = validateAndCleanRecord(record);
        
        if (cleanedRecord == null) {
            return;
        }

        // Filter by date > 2014 (only 2015 and later)
        String dateStr = cleanedRecord.get("date");
        if (dateStr == null || dateStr.isEmpty()) {
            return;
        }

        try {
            LocalDateTime arrestDate = LocalDateTime.parse(dateStr, OUTPUT_DATE_FORMATTER);
            if (arrestDate.isBefore(MIN_DATE)) {
                return;
            }
        } catch (DateTimeParseException e) {
            return;
        }

        String latitude = cleanedRecord.get("Latitude");
        String longitude = cleanedRecord.get("Longitude");
        String zipCode = null;
        
        if (latitude != null && longitude != null && 
            !latitude.isEmpty() && !longitude.isEmpty() &&
            !latitude.equals("0") && !longitude.equals("0")) {
            try {
                double lat = Double.parseDouble(latitude);
                double lon = Double.parseDouble(longitude);
                zipCode = zipCodeLookup.getZipCode(lat, lon);
            } catch (NumberFormatException e) {
            }
        }

        cleanedRecord.remove("Latitude");
        cleanedRecord.remove("Longitude");
        if (zipCode != null) {
            cleanedRecord.put("ZIP_CODE", zipCode);
        }

        // Build CSV row with fixed column order
        StringBuilder csvLine = new StringBuilder();
        for (int i = 0; i < OUTPUT_COLUMNS.length; i++) {
            String col = OUTPUT_COLUMNS[i];
            String val = cleanedRecord.getOrDefault(col, "");
            
            // Escape commas and quotes in values
            if (val.contains(",") || val.contains("\"") || val.contains("\n")) {
                val = "\"" + val.replace("\"", "\"\"") + "\"";
            }
            
            if (i > 0) {
                csvLine.append(",");
            }
            csvLine.append(val);
        }

        // Output CSV row (ignore key - whole row is in the value)
        context.write(NullWritable.get(), new Text(csvLine.toString()));
    }


    private Map<String, String> validateAndCleanRecord(Map<String, String> record) {
        Map<String, String> cleaned = new HashMap<>();
        
        for (Map.Entry<String, String> entry : record.entrySet()) {
            String columnName = entry.getKey();
            String value = entry.getValue();
            
            if (COLUMNS_TO_DROP.contains(columnName)) {
                continue;
            }
            
            if (value == null || value.isEmpty() || 
                value.equalsIgnoreCase("NA") || 
                value.equalsIgnoreCase("(null)") ||
                value.equalsIgnoreCase("null")) {
                continue;
            }
            
            String expectedType = SCHEMA.get(columnName);
            if (expectedType == null) {
                continue;
            }
            
            if (columnName.equals("ARREST_DATE")) {
                try {
                    LocalDateTime dateTime;
                    if (value.length() == 10) {
                        dateTime = LocalDateTime.parse(value + " 00:00:00", 
                            DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
                    } else {
                        dateTime = LocalDateTime.parse(value, 
                            DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
                    }
                    String formattedDate = dateTime.format(OUTPUT_DATE_FORMATTER);
                    cleaned.put("date", formattedDate);
                } catch (DateTimeParseException e) {
                    continue;
                }
                continue;
            }
            
            if (columnName.equals("ARREST_BORO")) {
                String borough = BORO_MAPPING.get(value.toUpperCase());
                if (borough != null) {
                    cleaned.put("borough", borough);
                } else {
                    cleaned.put("borough", value);
                }
                continue;
            }
            
            if (columnName.equals("LAW_CAT_CD")) {
                String category = LAW_CAT_MAPPING.get(value.toUpperCase());
                if (category != null) {
                    cleaned.put("Category_of_offense", category);
                } else {
                    cleaned.put("Category_of_offense", value);
                }
                continue;
            }
            
            if (columnName.equals("AGE_GROUP")) {
                String normalizedAge = normalizeAgeGroup(value);
                cleaned.put(columnName, normalizedAge);
                
                String[] parts = normalizedAge.split("-");
                if (parts.length == 2) {
                    try {
                        int minAge = Integer.parseInt(parts[0].trim());
                        int maxAge = Integer.parseInt(parts[1].trim());
                        cleaned.put("AGE_MIN", String.valueOf(minAge));
                        cleaned.put("AGE_MAX", String.valueOf(maxAge));
                    } catch (NumberFormatException e) {
                    }
                }
                continue;
            }
            
            if (columnName.equals("PD_CD") || columnName.equals("KY_CD")) {
                try {
                    double floatValue = Double.parseDouble(value);
                    int intValue = (int) floatValue;
                    cleaned.put(columnName, String.valueOf(intValue));
                } catch (NumberFormatException e) {
                    continue;
                }
                continue;
            }
            
            if (!isValidValue(value, expectedType)) {
                continue;
            }
            
            cleaned.put(columnName, value);
        }
        
        if (!cleaned.containsKey("ARREST_KEY") || cleaned.get("ARREST_KEY").isEmpty()) {
            return null;
        }
        
        if (!cleaned.containsKey("date")) {
            return null;
        }
        
        return cleaned;
    }

    private boolean isValidValue(String value, String expectedType) {
        try {
            switch (expectedType) {
                case "NUMBER":
                    Double.parseDouble(value);
                    return true;
                case "TEXT":
                    return true;
                case "TIMESTAMP":
                    return true;
                case "POINT":
                    return true;
                default:
                    return true;
            }
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String normalizeAgeGroup(String ageGroup) {
        if (ageGroup == null || ageGroup.isEmpty()) {
            return ageGroup;
        }
        
        if (ageGroup.equals("<18")) {
            return "0-17";
        }
        
        if (ageGroup.equals("65+") || ageGroup.equals("65-100")) {
            return "65-100";
        }
        
        return ageGroup.trim();
    }

    /**
     * Wrapper class for ZipcodeLookup to maintain compatibility with existing code.
     * Uses JTS point-in-polygon lookup instead of bounding box.
     */
    private static class ZipCodeLookup {
        private ZipcodeLookup zipcodeLookup;

        public ZipCodeLookup() {
            this.zipcodeLookup = new ZipcodeLookup();
        }

        public ZipCodeLookup(String csvFilePath, Configuration conf) throws IOException {
            try {
                this.zipcodeLookup = new ZipcodeLookup(csvFilePath, conf);
            } catch (Exception e) {
                System.err.println("Warning: Could not load zipcode CSV file: " + e.getMessage());
                this.zipcodeLookup = new ZipcodeLookup();
            }
        }

        public String getZipCode(double latitude, double longitude) {
            return zipcodeLookup.findZipcode(latitude, longitude);
        }
    }
}

