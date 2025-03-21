package analysis.productfeaturesanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductFeaturesDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ProductFeaturesDriver <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Features Analysis");
        job.setJarByClass(ProductFeaturesDriver.class);
        
        job.setMapperClass(ProductFeaturesMapper.class);
        job.setReducerClass(ProductFeaturesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));  // e.g., cleaned_metadata.csv
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
