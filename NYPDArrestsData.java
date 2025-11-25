import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NYPDArrestsData {
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2 && args.length != 3) {
            System.err.println("Usage: NYPDArrestsData <input path> <output path> [zipcode_bounds_file]");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(NYPDArrestsData.class);
        job.setJobName("NYPD Arrests Data Processing");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (args.length == 3) {
            job.getConfiguration().set("zipcode.bounds.file", args[2]);
        } else {
            job.getConfiguration().set("zipcode.bounds.file", "nyc_zip_data_lookup.csv");
        }

        job.setMapperClass(NYPDArrestsDataMapper.class);
        job.setReducerClass(NYPDArrestsDataReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        MultipleOutputs.addNamedOutput(job, "data", TextOutputFormat.class, 
            NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "stats", TextOutputFormat.class, 
            Text.class, Text.class);
        
        boolean success = job.waitForCompletion(true);
        
        if (success) {
            // Print counter summaries
            System.out.println("\n=== PREPROCESSING STATISTICS ===");
            System.out.println("Total Rows Processed: " + 
                job.getCounters().findCounter("STATS", "TOTAL_ROWS").getValue());
            System.out.println("Total Rows Dropped: " + 
                job.getCounters().findCounter("STATS", "DROPPED_ROWS").getValue());
            System.out.println("Zipcode Lookup Failed: " + 
                job.getCounters().findCounter("STATS", "ZIPCODE_LOOKUP_FAILED").getValue());
        }
        
        System.exit(success ? 0 : 1);
    }
}
