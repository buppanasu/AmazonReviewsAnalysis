package preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FinalMetaDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Final Meta Cleaning <input path> <output path>");
            System.exit(-1);
        }

        // Set up a new Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clean and Categorised Metadata");

        // Set the driver class
        job.setJarByClass(FinalMetaDriver.class);

        // Set the mapper and reducer classes
        job.setMapperClass(FinalMetaMapper.class);
        job.setReducerClass(FinalMetaReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
