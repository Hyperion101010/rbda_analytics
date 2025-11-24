import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
            job.getConfiguration().set("zipcode.bounds.file", "nyc_zipcode_bounds.json");
        }

        job.setMapperClass(NYPDArrestsDataMapper.class);
        job.setReducerClass(NYPDArrestsDataReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
