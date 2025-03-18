package analysis.reviewanomaly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReviewAnomalyDriver {
	
	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Review Anomaly Driver <input path> <output path>");
            System.exit(-1);
        }

        // Set up the Hadoop configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Review Count and Timestamps for Non-Verified Purchases");
        job.setJarByClass(ReviewAnomalyDriver.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(ReviewAnomalyMapper.class);
        job.setReducerClass(ReviewAnomalyReducer.class);

        // Set the output types for the Mapper and Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);  // Output path for the final result
        outPath.getFileSystem(conf).delete(outPath, true);  // Delete if it exists

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        // Exit the job if it fails
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
