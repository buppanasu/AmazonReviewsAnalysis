package analysis.q10uniquestores;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueStoreDriver {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
            System.err.println("Usage: Store Count <input> <output>");
            System.exit(2);
        }
		
        // Configuration and Job setup
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Store Count");

        // Set the Jar
        job.setJarByClass(UniqueStoreDriver.class);

        // Set Mapper and Reducer
        job.setMapperClass(UniqueStoreMapper.class);
        job.setReducerClass(UniqueStoreReducer.class);
        
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
