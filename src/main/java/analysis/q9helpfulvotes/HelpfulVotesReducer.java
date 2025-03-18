package analysis.q9helpfulvotes;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HelpfulVotesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Split the composite key into parent_asin and helpful_vote
        String[] keyParts = key.toString().split(",");
        String parentAsin = keyParts[0]; // Extract parent_asin
        int helpfulVote = Integer.parseInt(keyParts[1]); // Extract helpful_vote

        int highestHelpfulVote = Integer.MIN_VALUE;
        int highestHelpfulVoteCount = 0;

        // Iterate over all values for the given parent_asin and helpful_vote
        for (IntWritable value : values) {
            // If we find a higher helpful_vote, update the count and highestHelpfulVote
            if (helpfulVote > highestHelpfulVote) {
                highestHelpfulVote = helpfulVote;
                highestHelpfulVoteCount = value.get(); // Reset count for the highest helpful_vote
            } else if (helpfulVote == highestHelpfulVote) {
                highestHelpfulVoteCount += value.get(); // Increment the count for this helpful_vote
            }
        }

        // Emit only the highest helpful_vote for this parent_asin
        if (highestHelpfulVote != Integer.MIN_VALUE) {
            String outputKey = String.format("{\"parent_asin\":\"%s\", \"helpful_vote\":%d}", parentAsin, highestHelpfulVote);
            context.write(new Text(outputKey), new IntWritable(highestHelpfulVoteCount));
        }
    }
}
