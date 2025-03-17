package analysis.q7top10highestreviews;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class top10reviewsDriver {
	
	public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: top10reviewsDriver <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Product with most Reviews");
        job.setJarByClass(top10reviewsDriver.class);

        job.setMapperClass(top10reviewsMapper.class);
        job.setReducerClass(top10reviewsReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
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
