package analysis.q9helpfulvotes;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HelpfulVotesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line to get JSON content
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        String jsonLine = parts.length == 2 ? parts[1] : parts[0];

        try {
            // Parse JSON string into a JsonNode
            JsonNode root = objectMapper.readTree(jsonLine);
            JsonNode reviewsNode = root.path("reviews");
            String parentAsin = root.path("parent_asin").asText();
            
            // Iterate over each review in the reviews array
            for(JsonNode review : reviewsNode) {
                // Check if helpful_vote exists and is an integer
                if (review.has("helpful_vote") && review.get("helpful_vote").isInt()) {
                    int helpfulVote = review.get("helpful_vote").asInt();
                    int count = 1; // Count is 1 for each review

                    // Create a composite key: parent_asin, helpful_vote, count
                    String keyStr = parentAsin + "," + helpfulVote;
                    
                    // Emit the composite key with review data
                    context.write(new Text(keyStr), new IntWritable(count));
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
