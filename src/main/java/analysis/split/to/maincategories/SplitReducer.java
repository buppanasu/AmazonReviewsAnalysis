package analysis.split.to.maincategories;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SplitReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // The key is in the form: mainCategory,year
        String[] keyParts = key.toString().split(",");
        if (keyParts.length != 2) return;  // Skip if the key is malformed
        
        String mainCategory = keyParts[0]; // "Media", "Video Games"
        String year = keyParts[1]; // Year (e.g., 2019, 2020)
        
        // Accumulate all reviews for this mainCategory and year
        List<String> reviews = new ArrayList<>();
        for (Text value : values) {
            reviews.add(value.toString());
        }

        // Format the reviews as: Review: [Review1, Review2, ...]
        String reviewsList = String.join(", ", reviews);
        context.write(new Text(mainCategory + ", " + year), new Text("Review: [" + reviewsList + "]"));
    }
}
