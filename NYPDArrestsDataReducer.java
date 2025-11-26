import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NYPDArrestsDataReducer extends Reducer<Text, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    private long mx_grp_cnt = Long.MIN_VALUE;
    private String mx_age_grp_ky = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String key_val = key.toString();

        if (key_val.startsWith("AGE_GROUP:")) {
            long count = 0;
            for (Text value : values) {
                count++;
            }

            if (count > mx_grp_cnt) {
                mx_grp_cnt = count;
                mx_age_grp_ky = key_val;
            }
        }
        else if (key_val.startsWith("BOROUGH_YEAR:") || key_val.startsWith("ZIPCODE_TOTAL:") || key_val.startsWith("ZIPCODE_MISDEMEANOR:")) {
            long count = 0;
            for (Text value : values) {
                count++;
            }
            multipleOutputs.write("stats", key, new Text(String.valueOf(count)));
        } else if (key_val.equals("DATA:")) {
            for (Text value : values) {
                multipleOutputs.write("data", NullWritable.get(), value);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (mx_age_grp_ky != null) {
            String[] parts = mx_age_grp_ky.split(":");
            if (parts.length >= 2) {
                String ag_grp = parts[1];
                String opky = "AGE_GROUP_MAX:" + ag_grp;
                multipleOutputs.write("stats", new Text(opky), new Text(String.valueOf(mx_grp_cnt)));
            }
        }

        multipleOutputs.close();
    }
}
