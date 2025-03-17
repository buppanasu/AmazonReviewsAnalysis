package analysis.q7top10highestreviews;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class top10reviewsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    // TreeMap to store the top 10 products, sorted by rating_number
    private TreeMap<Integer, String> topProductsMap = new TreeMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int ratingNumber = key.get();

        for (Text value : values) {
            String productJson = value.toString();

            // Add the product and its rating_number to the TreeMap
            topProductsMap.put(ratingNumber, productJson);

            // If the TreeMap size exceeds 10, remove the product with the lowest rating_number
            if (topProductsMap.size() > 10) {
                topProductsMap.remove(topProductsMap.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Iterate over the TreeMap in descending order to get the top 10 products
        for (Integer ratingNumber : topProductsMap.descendingKeySet()) {
            context.write(new IntWritable(ratingNumber), new Text(topProductsMap.get(ratingNumber)));
        }
    }
}
