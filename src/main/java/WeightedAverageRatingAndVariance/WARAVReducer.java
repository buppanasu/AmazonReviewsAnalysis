package WeightedAverageRatingAndVariance;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WARAVReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumRating = 0.0;
        double sumRatingSquared = 0.0;
        long count = 0;

        // Accumulate totals
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 3) {
                double rating = Double.parseDouble(parts[0]);
                double ratingSquared = Double.parseDouble(parts[1]);
                long one = Long.parseLong(parts[2]);

                sumRating += rating;
                sumRatingSquared += ratingSquared;
                count += one;
            }
        }

        if (count > 0) {
            // Compute average
            double average = sumRating / count;
            // Compute variance
            double meanSquare = sumRatingSquared / count;
            double variance = meanSquare - (average * average);

            // Format output as JSON or a simple string
            String output = String.format("{\"average_rating\":%.2f,\"variance\":%.2f,\"review_count\":%d}",
                                          average, variance, count);

            context.write(key, new Text(output));
        }
    }
}
