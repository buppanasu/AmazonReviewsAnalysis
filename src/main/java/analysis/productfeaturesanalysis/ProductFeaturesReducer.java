package analysis.productfeaturesanalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductFeaturesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // If a product appears more than once, sum the counts.
        int totalFeatures = 0;
        for (Text val : values) {
            try {
                totalFeatures += Integer.parseInt(val.toString());
            } catch(NumberFormatException e) {
                // Skip any unparsable values.
            }
        }
        context.write(key, new Text(String.valueOf(totalFeatures)));
    }
}