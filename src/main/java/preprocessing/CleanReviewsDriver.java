package preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CleanReviewsDriver {
    public static void main(String[] args) throws Exception {
        // Create Hadoop configuration and force local mode
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        // Parse command-line arguments
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: CleanReviewsDriver <input path> <output path>");
            System.exit(2);
        }
        
        // Create and configure the job
        Job job = Job.getInstance(conf, "Clean Reviews");
        job.setJarByClass(CleanReviewsDriver.class);
        job.setMapperClass(CleanReviewsMapper.class);
        job.setReducerClass(DedupReducer.class);
        
        // Force a single reducer for sequential numbering
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        // Delete Output Path if there is one.
        outPath.getFileSystem(conf).delete(outPath, true);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        
        // Submit the job and exit based on its success or failure.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


