package analysis.q9helpfulvotes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HelpfulVotesDriver {

	public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: HelpfulVotes <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reviews with the most Helpful Votes");
        job.setJarByClass(HelpfulVotesDriver.class);

        job.setMapperClass(HelpfulVotesMapper.class);
        job.setReducerClass(HelpfulVotesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setNumReduceTasks(1);
        
        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        // Delete Output Path if there is one.
        outPath.getFileSystem(conf).delete(outPath, true);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);


        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
	
}
