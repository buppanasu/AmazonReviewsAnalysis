package analysis.q9highesthelpfulvotes;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighestHelpfulVoteMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text productID = new Text();
    private IntWritable helpfulVote = new IntWritable();
    private ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	// Split the line to get JSON content
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        String jsonLine = parts.length == 2 ? parts[1] : parts[0];
    	
        try {
        	// Parse JSON string into a JsonNode
            JsonNode root = objectMapper.readTree(jsonLine);
            JsonNode reviewsNode = root.path("reviews");
            String parentAsin = root.path("parent_asin").asText();

            // For each review, emit the helpful_vote and parent_asin
            for (JsonNode review : reviewsNode) {
                int vote = review.get("helpful_vote").asInt();
                productID.set(parentAsin);
                helpfulVote.set(vote);
                context.write(productID, helpfulVote);
            }

        } catch (Exception e) {
            // Log the error for debugging
            e.printStackTrace();
        }
    }
}
