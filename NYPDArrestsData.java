import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class NYPDArrestsData {
    public static void main(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: NYPDArrestsData <input path> <output path> <zipcode lookup file>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(NYPDArrestsData.class);
        job.setJobName("NYPD Arrests Data Processing");
        
        // Setting configurations for zipcode file
        // reference - https://stackoverflow.com/questions/13228922/setting-parameter-in-mapreduce-job-configuration
        Configuration conf = new Configuration();
        conf.set("zipcode_lookup_file", args[2]);
        job.setConfiguration(conf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NYPDArrestsDataMapper.class);
        job.setReducerClass(NYPDArrestsDataReducer.class);
        job.setNumReduceTasks(1);

        // This enables me to define specific output types for mapper and reducer
        // Since my mapper and reducer output types differ
        // reference - https://stackoverflow.com/questions/38376688/why-setmapoutputkeyclass-method-is-necessary-in-mapreduce-job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        // Write to data for my data output
        // Write to stats for my statistics information
        MultipleOutputs.addNamedOutput(job, "data", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "stats", TextOutputFormat.class, Text.class, Text.class);

        boolean success = job.waitForCompletion(true);

        if(success){
            long total_row = job.getCounters().findCounter("STATS", "TOTAL_ROWS").getValue();
            long allowed_row = job.getCounters().findCounter("STATS", "ALLOWED_ROW").getValue();
            System.out.println("Total Rows Processed: " + total_row);
            System.out.println("Total Allowed Rows: " + allowed_row);
        }

        System.exit(success ? 0 : 1);
    }
}
