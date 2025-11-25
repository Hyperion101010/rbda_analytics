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
        
        if (args.length != 3) {
            System.err.println("Usage: NYPDArrestsData <input path> <output path> <zipcode lookup file>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(NYPDArrestsData.class);
        job.setJobName("NYPD Arrests Data Processing");
        
        // Set zipcode file path in configuration
        System.out.println("=== ZIPCODE FILE CONFIGURATION ===");
        System.out.println("Before setting: zipcode.bounds.file = " + 
            job.getConfiguration().get("zipcode.bounds.file"));
        System.out.println("Setting zipcode file path from args[2]: " + args[2]);
        
        job.getConfiguration().set("zipcode.bounds.file", args[2]);
        
        System.out.println("After setting: zipcode.bounds.file = " + 
            job.getConfiguration().get("zipcode.bounds.file"));
        System.out.println("===================================");
        System.out.println();
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
            System.out.println("\n=== STATISTICS ===");
            System.out.println("Total Rows Processed: " + 
                job.getCounters().findCounter("STATS", "TOTAL_ROWS").getValue());
            System.out.println("Total Rows Dropped: " + 
                job.getCounters().findCounter("STATS", "DROPPED_ROWS").getValue());
            System.out.println("Invalid ZIPCODEs: " + 
                job.getCounters().findCounter("STATS", "INVALID_ZIPCODES").getValue());
        }
        
        System.exit(success ? 0 : 1);
    }
}
