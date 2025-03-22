package analysis.topengagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopEngagementDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopEngagementDriver <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Engagement Products");
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
        job.setJarByClass(TopEngagementDriver.class);
        
        job.setMapperClass(TopEngagementMapper.class);
        job.setReducerClass(TopEngagementReducer.class);
        
        // The mapper emits Text,Text and reducer emits Text,Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.out.println("[DRIVER] Job started");
        boolean success = job.waitForCompletion(true);
        System.out.println("[DRIVER] Job completed: " + success);
        System.exit(success ? 0 : 1);
    }
}
