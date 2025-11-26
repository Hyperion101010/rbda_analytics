import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class NYPDArrestsDataReducer extends Reducer<Text, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String key_val = key.toString();

        if (key_val.startsWith("BOROUGH_YEAR:") || key_val.startsWith("ZIPCODE_YEAR:") || key_val.startsWith("ZIPCODE_YEAR_DAY:") || key_val.startsWith("ZIPCODE_TOTAL:") || key_val.startsWith("ZIPCODE_MISDEMEANOR:")) {
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
        multipleOutputs.close();
    }
}
