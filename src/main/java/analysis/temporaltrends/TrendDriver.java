package analysis.temporaltrends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrendDriver {

	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Trend Driver <input path> <output path>");
            System.exit(-1);
        }

        // Set up the Hadoop configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Review Average Rating and Count");
        job.setJarByClass(TrendDriver.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(TrendMapper.class);
        job.setReducerClass(TrendReducer.class);

        // Set the output types for the Mapper and Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        // Delete Output Path if there is one.
        outPath.getFileSystem(conf).delete(outPath, true);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        // Exit the job if it fails
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
