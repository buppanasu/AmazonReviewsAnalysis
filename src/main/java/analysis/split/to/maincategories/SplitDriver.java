package analysis.split.to.maincategories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SplitDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Main Category Split <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Main Category Split");
        job.setJarByClass(SplitDriver.class);
        
        // Set the Mapper and Reducer classes
        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(SplitReducer.class);
        
        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set the partitioner class to partition by main_category
        job.setPartitionerClass(SplitPartitioner.class);
        
        // Set the number of reducers to be at least 10 (one for each category)
        job.setNumReduceTasks(10);
        
        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);  // Output path for the final result
        
        outPath.getFileSystem(conf).delete(outPath, true);  // Delete if it exists

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        
        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

