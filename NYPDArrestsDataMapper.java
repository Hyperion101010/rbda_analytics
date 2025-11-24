import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    private List<String> columnNames;
    private ZipCodeLookup zipCodeLookup;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();
        String zipcodeFile = conf.get("zipcode.bounds.file", "nyc_zipcode_bounds.json");
        
        try {
            zipCodeLookup = new ZipCodeLookup(zipcodeFile, conf);
        } catch (Exception e) {
            System.err.println("Warning: Could not load zipcode bounds file: " + e.getMessage());
            zipCodeLookup = new ZipCodeLookup();
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        if (line.isEmpty()) {
            return;
        }

        List<String> fields = parseCSVLine(line);
        
        if (key.get() == 0) {
            columnNames = new ArrayList<>(fields);
            for (int i = 0; i < columnNames.size(); i++) {
                columnNames.set(i, columnNames.get(i).replaceAll("^\"|\"$", ""));
            }
            return;
        }

        if (columnNames == null || fields.size() != columnNames.size()) {
            return;
        }

        Map<String, String> record = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i);
            String fieldValue = fields.get(i).replaceAll("^\"|\"$", "").trim();
            record.put(colName, fieldValue);
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

        StringBuilder output = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : cleanedRecord.entrySet()) {
            if (!first) {
                output.append(",");
            }
            output.append(entry.getKey()).append(":").append(entry.getValue());
            first = false;
        }

        String arrestKey = cleanedRecord.get("ARREST_KEY");
        if (arrestKey != null && !arrestKey.isEmpty()) {
            context.write(new Text(arrestKey), new Text(output.toString()));
        }
    }

    private List<String> parseCSVLine(String line) {
        List<String> fields = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentField = new StringBuilder();
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }
        fields.add(currentField.toString());
        
        return fields;
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
        
        return ageGroup.trim();
    }

    private static class ZipCodeLookup {
        private List<ZipCodeBound> zipCodeBounds;

        public ZipCodeLookup() {
            this.zipCodeBounds = new ArrayList<>();
        }

        public ZipCodeLookup(String jsonFilePath, Configuration conf) throws IOException {
            this.zipCodeBounds = new ArrayList<>();
            loadZipCodeBounds(jsonFilePath, conf);
        }

        private void loadZipCodeBounds(String jsonFilePath, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(jsonFilePath);
            
            InputStream inputStream = null;
            try {
                if (fs.exists(path)) {
                    inputStream = fs.open(path);
                } else {
                    java.io.File localFile = new java.io.File(jsonFilePath);
                    if (localFile.exists()) {
                        inputStream = new java.io.FileInputStream(localFile);
                    } else {
                        throw new IOException("Zipcode bounds file not found: " + jsonFilePath);
                    }
                }

                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = mapper.readTree(inputStream);
                JsonNode zipcodesNode = rootNode.get("zipcodes");

                if (zipcodesNode != null && zipcodesNode.isArray()) {
                    for (JsonNode zipNode : zipcodesNode) {
                        String zipcode = zipNode.get("zipcode").asText();
                        JsonNode boundsNode = zipNode.get("bounds");
                        
                        if (boundsNode != null) {
                            double minLat = boundsNode.get("min_lat").asDouble();
                            double maxLat = boundsNode.get("max_lat").asDouble();
                            double minLon = boundsNode.get("min_lon").asDouble();
                            double maxLon = boundsNode.get("max_lon").asDouble();
                            
                            zipCodeBounds.add(new ZipCodeBound(zipcode, minLat, maxLat, minLon, maxLon));
                        }
                    }
                }
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }

        public String getZipCode(double latitude, double longitude) {
            for (ZipCodeBound bound : zipCodeBounds) {
                if (bound.contains(latitude, longitude)) {
                    return bound.zipcode;
                }
            }
            return null;
        }

        private static class ZipCodeBound {
            String zipcode;
            double minLat;
            double maxLat;
            double minLon;
            double maxLon;

            public ZipCodeBound(String zipcode, double minLat, double maxLat, double minLon, double maxLon) {
                this.zipcode = zipcode;
                this.minLat = minLat;
                this.maxLat = maxLat;
                this.minLon = minLon;
                this.maxLon = maxLon;
            }

            public boolean contains(double lat, double lon) {
                return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
            }
        }
    }
}

