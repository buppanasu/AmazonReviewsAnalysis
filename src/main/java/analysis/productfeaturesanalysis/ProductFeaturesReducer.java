package analysis.productfeaturesanalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductFeaturesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // If a product appears more than once, you might want to decide how to combine them.
        // For now, we'll simply take the first occurrence.
        context.write(key, values.iterator().next());
    }
}
