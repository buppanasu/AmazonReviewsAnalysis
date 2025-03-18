package analysis.temporaltrends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text outputValue = new Text();
	private Text outputKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalRating = 0;
        int totalCount = 0;

        // Iterate through all the values and sum the ratings and counts
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            double rating = Double.parseDouble(parts[0]);
            int count = Integer.parseInt(parts[1]);

            totalRating += rating;
            totalCount += count;
        }

        // Calculate average rating
        double averageRating = totalCount > 0 ? totalRating / totalCount : 0;

        // Extract the parent_asin and time-period from the key
        String[] keyParts = key.toString().split(",");
        String parentAsin = keyParts[0];
        String timePeriod = keyParts[1];
        
        String Key = " \"parent_asin\":" + parentAsin +
        				  ", \"time_period\":" + timePeriod;

        // Construct the final output string in the desired format
        String result = "\"average_rating\":" + String.format("%.2f", averageRating) +
                        ", \"rating_count\":" + totalCount;

        // Set the final result to the output value
        outputKey.set(Key);
        outputValue.set(result);

        // Emit the final result
        context.write(outputKey, outputValue);
    }
	
}
