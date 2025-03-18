package analysis.temporaltrends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrendDriver {

	public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Trend Driver <input path> <output_ALL path> <output_True_Only path>");
            System.exit(-1);
        }

        // Set up the Hadoop configuration
        Configuration conf = new Configuration();
        Job jobAllReviews = Job.getInstance(conf, "Review Average Rating and Count for ALL Reviews");
        jobAllReviews.setJarByClass(TrendDriver.class);

        // Set the Mapper and Reducer classes
        jobAllReviews.setMapperClass(TrendMapper.class);
        jobAllReviews.setReducerClass(TrendReducer.class);

        // Set the output types for the Mapper and Reducer
        jobAllReviews.setOutputKeyClass(Text.class);
        jobAllReviews.setOutputValueClass(Text.class);
        
        Job jobTrueOnlyReviews = Job.getInstance(conf, 
        		"Review Average Rating and Count for Verified Purchase Reviews");
        jobTrueOnlyReviews.setJarByClass(TrendDriver.class);
        
        jobTrueOnlyReviews.setMapperClass(TrueOnlyTrendMapper.class);
        jobTrueOnlyReviews.setReducerClass(TrendReducer.class);
        
        // Set the output types for the Mapper and Reducer
        jobTrueOnlyReviews.setOutputKeyClass(Text.class);
        jobTrueOnlyReviews.setOutputValueClass(Text.class);

        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outALLPath = new Path(args[1]);
        Path outTrueOnlyPath = new Path(args[2]);
        
        // Delete Output Path if there is one.
        outALLPath.getFileSystem(conf).delete(outALLPath, true);
        outTrueOnlyPath.getFileSystem(conf).delete(outTrueOnlyPath, true);

        FileInputFormat.addInputPath(jobAllReviews, inPath);
        FileOutputFormat.setOutputPath(jobAllReviews, outALLPath);
        
        FileInputFormat.addInputPath(jobTrueOnlyReviews, inPath);
        FileOutputFormat.setOutputPath(jobTrueOnlyReviews, outTrueOnlyPath);

        // Run both jobs sequentially
        if (jobAllReviews.waitForCompletion(true)) {
            System.exit(jobTrueOnlyReviews.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }

}
