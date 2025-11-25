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

public class NYPDArrestsDataMapper extends Mapper<LongWritable, Text, Text, Text> {

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
        Map.entry("Lon_Lat", "POINT"),
        Map.entry("ZIPCODE", "NUMBER")
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
        "ARREST_DATE",
        "OFNS_DESC",
        "PD_CD",
        "KY_CD",
        "LAW_CAT_CD",
        "ARREST_BORO",
        "ARREST_PRECINCT",
        "AGE_GROUP",
        "AGE_MIN",
        "AGE_MAX",
        "PERP_SEX",
        "JURISDICTION_CODE",
        "PD_DESC",
        "LAW_CODE",
        "ZIPCODE"
    };

    // Reference to column index mapping
    private static final Map<String, Integer> COL = CsvSchema.COL;
    
    private CSVParser csvParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        csvParser = new CSVParserBuilder()
                .withSeparator(',')   // standard CSV
                .withQuoteChar('"')   // handle "quoted, fields"
                .withEscapeChar('\\') // allow \" inside
                .build();
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

        // Track statistics - check if record will be dropped
        String originalDate = record.get("ARREST_DATE");
        boolean willBeDropped = false;
        String dropReason = "";
        
        if (record.get("ARREST_KEY") == null || record.get("ARREST_KEY").isEmpty()) {
            willBeDropped = true;
            dropReason = "MISSING_ARREST_KEY";
        } else if (originalDate == null || originalDate.isEmpty()) {
            willBeDropped = true;
            dropReason = "MISSING_DATE";
        } else {
            try {
                LocalDateTime dateTime;
                if (originalDate.length() == 10) {
                    dateTime = LocalDateTime.parse(originalDate + " 00:00:00", 
                        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
                } else {
                    dateTime = LocalDateTime.parse(originalDate, 
                        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
                }
                if (dateTime.isBefore(MIN_DATE)) {
                    willBeDropped = true;
                    dropReason = "DATE_BEFORE_2015";
                }
            } catch (Exception e) {
                willBeDropped = true;
                dropReason = "INVALID_DATE";
            }
        }

        Map<String, String> cleanedRecord = validateAndCleanRecord(record);
        
        if (cleanedRecord == null) {
            context.getCounter("STATS", "DROPPED_ROWS").increment(1);
            if (!dropReason.isEmpty()) {
                context.getCounter("STATS", "DROP_REASON_" + dropReason).increment(1);
            }
            return;
        }

        // Filter by date > 2014 (only 2015 and later)
        String dateStr = cleanedRecord.get("ARREST_DATE");
        if (dateStr == null || dateStr.isEmpty()) {
            context.getCounter("STATS", "DROPPED_ROWS").increment(1);
            context.getCounter("STATS", "DROP_REASON_MISSING_DATE").increment(1);
            return;
        }

        LocalDateTime arrestDate;
        try {
            arrestDate = LocalDateTime.parse(dateStr, OUTPUT_DATE_FORMATTER);
            if (arrestDate.isBefore(MIN_DATE)) {
                context.getCounter("STATS", "DROPPED_ROWS").increment(1);
                context.getCounter("STATS", "DROP_REASON_DATE_BEFORE_2015").increment(1);
                return;
            }
        } catch (DateTimeParseException e) {
            context.getCounter("STATS", "DROPPED_ROWS").increment(1);
            context.getCounter("STATS", "DROP_REASON_INVALID_DATE").increment(1);
            return;
        }
        
        // Record passed validation
        context.getCounter("STATS", "TOTAL_ROWS").increment(1);

        // Validate ZIPCODE - must be NYC zipcode (100xx to 116xx) or drop row
        String zipcode = cleanedRecord.get("ZIPCODE");
        if (!isValidNYCZipcode(zipcode)) {
            context.getCounter("STATS", "DROPPED_ROWS").increment(1);
            context.getCounter("STATS", "DROP_REASON_INVALID_ZIPCODE").increment(1);
            return;
        }

        // Remove Latitude and Longitude columns
        cleanedRecord.remove("Latitude");
        cleanedRecord.remove("Longitude");

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

        // Emit cleaned CSV row with special key for routing (will go through reducer to data output)
        context.write(new Text("DATA:"), new Text(csvLine.toString()));

        // Emit statistics as key-value pairs for aggregation
        String year = String.valueOf(arrestDate.getYear());
        String borough = cleanedRecord.get("ARREST_BORO");
        String dateStrFormatted = arrestDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        
        // Crimes per Borough per Year
        if (borough != null && !borough.isEmpty()) {
            context.write(
                new Text("BOROUGH_YEAR:" + borough + ":" + year), 
                new Text("1"));
        }
        
        // Records per day
        context.write(
            new Text("DAILY:" + dateStrFormatted), 
            new Text("1"));
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
                    cleaned.put("ARREST_DATE", formattedDate);
                } catch (DateTimeParseException e) {
                    continue;
                }
                continue;
            }
            
            if (columnName.equals("ARREST_BORO")) {
                String borough = BORO_MAPPING.get(value.toUpperCase());
                if (borough != null) {
                    cleaned.put("ARREST_BORO", borough);
                } else {
                    cleaned.put("ARREST_BORO", value);
                }
                continue;
            }
            
            if (columnName.equals("LAW_CAT_CD")) {
                String category = LAW_CAT_MAPPING.get(value.toUpperCase());
                if (category != null) {
                    cleaned.put("LAW_CAT_CD", category);
                } else {
                    cleaned.put("LAW_CAT_CD", value);
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
        
        if (!cleaned.containsKey("ARREST_DATE")) {
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
     * Validates if a zipcode is a valid NYC zipcode.
     * NYC zipcodes fall within the range 10000-11699 (100xx to 116xx).
     * @param zipcode the zipcode string to validate
     * @return true if zipcode is valid NYC zipcode, false otherwise (including null/empty)
     */
    private boolean isValidNYCZipcode(String zipcode) {
        // Drop if null or empty
        if (zipcode == null || zipcode.trim().isEmpty()) {
            return false;
        }

        try {
            // Parse as integer
            int zip = Integer.parseInt(zipcode.trim());
            
            // NYC zipcodes: 10000 to 11699 (100xx to 116xx)
            return zip >= 10000 && zip <= 11699;
        } catch (NumberFormatException e) {
            // Not a valid integer, drop the row
            return false;
        }
    }
}

