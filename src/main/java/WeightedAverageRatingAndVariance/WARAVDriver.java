package WeightedAverageRatingAndVariance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WARAVDriver {
	
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WARAVDriver <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weighted Average Rating & Variance");
        job.setJarByClass(WARAVDriver.class);

        // Set Mapper and Reducer
        job.setMapperClass(WARAVMapper.class);
        job.setReducerClass(WARAVReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and Output format
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
