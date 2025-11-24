import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class ArrestRecordWritable implements Writable {
    private Map<String, String> fields;

    public ArrestRecordWritable() {
        this.fields = new HashMap<>();
    }

    public ArrestRecordWritable(Map<String, String> fields) {
        this.fields = fields != null ? new HashMap<>(fields) : new HashMap<>();
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields != null ? new HashMap<>(fields) : new HashMap<>();
    }

    public String get(String key) {
        return fields.get(key);
    }

    public void put(String key, String value) {
        fields.put(key, value);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(fields.size());
        
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue() != null ? entry.getValue() : "");
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fields.clear();
        
        int numFields = in.readInt();
        
        for (int i = 0; i < numFields; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            fields.put(key, value.isEmpty() ? null : value);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (!first) {
                sb.append("\t");
            }
            String value = entry.getValue() != null ? entry.getValue().replace("\t", "\\t") : "";
            sb.append(entry.getKey()).append(":").append(value);
            first = false;
        }
        return sb.toString();
    }
}
