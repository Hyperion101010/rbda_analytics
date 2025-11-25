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
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {

        String keyStr = key.toString();
        
        // If it's a stats key, aggregate and write to stats output
        if (keyStr.startsWith("BOROUGH_YEAR:") || keyStr.startsWith("DAILY:")) {
            long count = 0;
            for (Text value : values) {
                count++;
            }
            multipleOutputs.write("stats", key, new Text(String.valueOf(count)));
        } else if (keyStr.equals("DATA:")) {
            // It's data, pass through to data output
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
