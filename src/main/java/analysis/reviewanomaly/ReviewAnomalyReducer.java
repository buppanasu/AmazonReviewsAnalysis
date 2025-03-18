package analysis.reviewanomaly;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ReviewAnomalyReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputValue = new Text();
    private Text outputKey = new Text();

    // List to store review count and timestamps for each user_id
    private List<Object[]> userReviewsList = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int reviewCount = 0;
        List<Long> timestamps = new ArrayList<>();

        // Iterate through all the values and sum the reviews and collect the timestamps
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            int count = Integer.parseInt(parts[0]);
            long timestamp = Long.parseLong(parts[1]);

            reviewCount += count;
            timestamps.add(timestamp);
        }

        // Add the user_id's review count and timestamps to the list for sorting in cleanup
        userReviewsList.add(new Object[]{reviewCount, timestamps, key.toString()});
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort the list by review_count in descending order
        Collections.sort(userReviewsList, new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                return Integer.compare((int) o2[0], (int) o1[0]); // Descending order by review_count
            }
        });

        // Emit the sorted results
        for (Object[] entry : userReviewsList) {
            int reviewCount = (int) entry[0];
            List<Long> timestamps = (List<Long>) entry[1];
            String userId = (String) entry[2];

            String result = "{\"review_count\":" + reviewCount +
                            ", \"timestamps\":" + timestamps.toString() + "}";

            outputKey.set("\"user_id\":" + userId);
            outputValue.set(result);

            // Emit the final result
            context.write(outputKey, outputValue);
        }
    }
}
