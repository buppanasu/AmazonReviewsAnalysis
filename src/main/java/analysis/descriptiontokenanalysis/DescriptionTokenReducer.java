package analysis.descriptiontokenanalysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DescriptionTokenReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int productCount = 0;
        int totalRatings = 0;
        for (IntWritable val : values) {
            productCount++;
            totalRatings += val.get();
        }
        double averageRating = productCount > 0 ? (double) totalRatings / productCount : 0.0;
        // Format the output as: productCount,totalRatings,averageRating (formatted to 2 decimals)
        result.set(productCount + "," + totalRatings + "," + String.format("%.2f", averageRating));
        context.write(key, result);
    }
}
