package analysis.ProductMetadataAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductMetadataDriver {
	
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ProductMetadataDriver <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Metadata Analysis");
        job.setJarByClass(ProductMetadataDriver.class);
        
        job.setMapperClass(ProductMetadataMapper.class);
        job.setReducerClass(ProductMetadataReducer.class);
        
        // Set output key and value types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
