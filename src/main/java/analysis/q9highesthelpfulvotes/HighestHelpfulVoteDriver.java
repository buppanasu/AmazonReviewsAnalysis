package analysis.q9highesthelpfulvotes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestHelpfulVoteDriver {

    public static void main(String[] args) throws Exception {
        // Configuration and Job setup
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Highest Helpful Vote Count");

        // Set the Jar
        job.setJarByClass(HighestHelpfulVoteDriver.class);

        // Set Mapper and Reducer
        job.setMapperClass(HighestHelpfulVoteMapper.class);
        job.setReducerClass(HighestHelpfulVoteReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); // Output value is now Text (JSON string)
        
        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        // Delete Output Path if there is one.
        outPath.getFileSystem(conf).delete(outPath, true);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
