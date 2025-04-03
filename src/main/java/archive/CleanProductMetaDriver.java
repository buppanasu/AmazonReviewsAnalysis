package preprocessing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CleanProductMetaDriver {

    public static void main(String[] args) throws Exception {
        // Create Hadoop configuration and force local mode
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        // Parse command-line arguments
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: CleanProductMetaDriver <input path> <output path>");
            System.exit(2);
        }

        // Create and configure the job
        Job job = Job.getInstance(conf, "Clean Product Metadata");
        job.setJarByClass(CleanProductMetaDriver.class);

        // Use the CleanProductMetaMapper for metadata cleaning
        job.setMapperClass(CleanProductMetaMapper.class);

        // Use the same DedupReducer (can be a separate class or the same used in CleanReviewsDriver)
        job.setReducerClass(DedupReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Submit the job and exit based on its success or failure.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * DedupReducer: For each composite key, output only one cleaned record.
     * This effectively removes duplicate records.
     */
    public static class DedupReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws java.io.IOException, InterruptedException {
            // Emit only the first record for each composite key
            context.write(null, values.iterator().next());
        }
    }
}
