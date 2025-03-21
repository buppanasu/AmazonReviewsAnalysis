package final_merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FinalJoinDriver {
    public static void main(String[] args) throws Exception {
        System.out.println("MergeReviewsAndMetaDriver: Starting merge job...");

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 3) {
            System.err.println("Usage: MergeReviewsAndMetaDriver <clean reviews input> <clean meta input> <output path>");
            System.exit(2);
        }
        
        String reviewsInput = otherArgs[0];
        String metaInput = otherArgs[1];
        String outputDir = otherArgs[2];
        
        System.out.println("MergeReviewsAndMetaDriver: Reviews input = " + reviewsInput);
        System.out.println("MergeReviewsAndMetaDriver: Metadata input = " + metaInput);
        System.out.println("MergeReviewsAndMetaDriver: Output path = " + outputDir);
        
        // Delete output directory if it exists
        Path outputPath = new Path(outputDir);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            System.out.println("MergeReviewsAndMetaDriver: Output directory exists. Deleting " + outputDir);
            fs.delete(outputPath, true);
        }

        // Create job
        Job job = Job.getInstance(conf, "Merge Reviews and Product Metadata");
        job.setJarByClass(FinalJoinDriver.class);

        // Input paths for reviews and metadata
        MultipleInputs.addInputPath(job, new Path(reviewsInput),
                TextInputFormat.class, FinalReviewJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(metaInput),
                TextInputFormat.class, FinalMetaJoinMapper.class);
        
        job.setReducerClass(FinalJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, outputPath);
        
        System.out.println("MergeReviewsAndMetaDriver: Starting job submission...");
        boolean success = job.waitForCompletion(true);
        System.out.println("MergeReviewsAndMetaDriver: Job completed with status: " + success);
        
        if (success) {
            System.out.println("Merge job completed successfully. Output is available at: " + outputDir);
            System.exit(0);
        } else {
            System.err.println("Merge job failed. Please see the error messages or stack trace above.");
            System.exit(1);
        }
    }
}
