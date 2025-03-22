package analysis.descriptiontokenanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DescriptionTokenDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: DescriptionTokenAnalysis <input path> <output path> <stopwords file path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        // Pass stopwords file path as a configuration parameter.
        conf.set("stopwords.path", args[2]);

        Job job = Job.getInstance(conf, "Description Token Analysis");
        job.setJarByClass(DescriptionTokenDriver.class);

        job.setMapperClass(DescriptionTokenMapper.class);
        job.setReducerClass(DescriptionTokenReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
