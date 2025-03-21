package analysis.ProductMetadataAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductMetadataReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // If a product appears more than once, you can decide how to merge them.
        // For this example, we assume each product appears only once.
        for (Text val : values) {
            context.write(key, val);
            // If there are multiple records per product, you might choose to aggregate them.
        }
    }
}
