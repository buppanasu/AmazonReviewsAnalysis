package analysis.sentimentanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysisDriver {
	
	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReviewJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amazon Reviews Extractor");

        job.setJarByClass(SentimentAnalysisDriver.class);
        job.setMapperClass(SentimentAnalysisMapper.class);
        job.setReducerClass(SentimentAnalysisReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Define Input & Output Paths
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        
        // Delete Output Path if there is one.
        outPath.getFileSystem(conf).delete(outPath, true);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
