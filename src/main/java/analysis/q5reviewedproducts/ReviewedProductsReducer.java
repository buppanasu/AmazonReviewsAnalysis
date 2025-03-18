package analysis.q5reviewedproducts;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewedProductsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Counter for unique products with at least one review
    private int reviewedCount = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // Each call here represents one unique product (parent_asin)
        reviewedCount++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit a single key-value pair summarizing the total count
        context.write(new Text("Total products with at least one review"), new IntWritable(reviewedCount));
    }
}