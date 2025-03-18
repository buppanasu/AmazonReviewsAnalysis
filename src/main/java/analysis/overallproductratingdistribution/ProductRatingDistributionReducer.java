package analysis.overallproductratingdistribution;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductRatingDistributionReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private final IntWritable result = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
