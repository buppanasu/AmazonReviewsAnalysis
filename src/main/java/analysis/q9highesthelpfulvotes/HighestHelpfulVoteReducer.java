package analysis.q9highesthelpfulvotes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class HighestHelpfulVoteReducer extends Reducer<Text, IntWritable, Text, Text> {

    private ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxVote = Integer.MIN_VALUE;
        int count = 0;

        // Find the highest vote and count its occurrences
        for (IntWritable value : values) {
            int vote = value.get();
            if (vote > maxVote) {
                maxVote = vote;
                count = 1; // Reset the count for the new max vote
            } else if (vote == maxVote) {
                count++; // Increment the count if the current vote matches the max vote
            }
        }

        // Create a LinkedHashMap to hold helpful_vote and count in the desired order
        Map<String, Object> resultMap = new LinkedHashMap<>();
        resultMap.put("helpful_vote", maxVote);  // Add helpful_vote first
        resultMap.put("count", count);           // Add count second

        // Convert the map to a JSON string using Jackson
        String jsonOutput = objectMapper.writeValueAsString(resultMap);

        // Emit the result: parent_asin -> {"helpful_vote": ..., "count": ...}
        context.write(key, new Text(jsonOutput));
    }
}

