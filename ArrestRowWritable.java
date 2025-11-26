import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

// reference - https://labex.io/tutorials/hadoop-how-to-create-a-custom-writable-class-to-represent-data-in-hadoop-mapreduce-415101

public class ArrestRowWritable implements Writable {
    private LinkedHashMap<String, String> each_arrest_data;

    public ArrestRowWritable() {
        this.each_arrest_data = new LinkedHashMap<>();
    }

    public String get_vl(String ky) {
        return this.each_arrest_data.get(ky);
    }

    public void set_vl(String ky, String vl) {
        this.each_arrest_data.put(ky, vl);
    }

    @Override
    public void write(DataOutput out) throws IOException {

        // First lets write the total number of key and value pairs
        // That's how we will deserialize it later
        out.writeInt(this.each_arrest_data.size());

        Iterator<LinkedHashMap.Entry<String, String>> each_ele = this.each_arrest_data.entrySet().iterator();

        while(each_ele.hasNext()) {
            LinkedHashMap.Entry<String, String> each_entr = each_ele.next();
            out.writeUTF(each_entr.getKey());

            String my_vl = each_entr.getValue();

            if (my_vl == null) {
                my_vl = "";
            }
            out.writeUTF(my_vl);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.each_arrest_data.clear();

        // Our first element is always the count of elements in the object.
        int total_eles = in.readInt();

        for (int i = 0; i < total_eles; i++) {
            String ky = in.readUTF();
            String vl = in.readUTF();

            if (vl.equals("")) {
                vl = null;
            }
            this.each_arrest_data.put(ky, vl);
        }
    }
}