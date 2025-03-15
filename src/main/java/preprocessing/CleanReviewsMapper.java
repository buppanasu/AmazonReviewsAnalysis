package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CleanReviewsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ObjectMapper objectMapper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize Jackson ObjectMapper for JSON parsing
        objectMapper = new ObjectMapper();
        System.out.println("DEBUG: CleanReviewsMapper setup() called - ObjectMapper initialized.");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String jsonLine = value.toString();
        // Print the raw line to see exactly what's being read
        System.out.println("DEBUG: Raw input line -> " + jsonLine);

        try {
            // Parse the JSON line into a JsonNode
            JsonNode rootNode = objectMapper.readTree(jsonLine);

            // Check verified_purchase
            if (!rootNode.hasNonNull("verified_purchase") || !rootNode.get("verified_purchase").asBoolean()) {
                System.out.println("DEBUG: Skipping record - 'verified_purchase' is missing or false.");
                return;
            }

            // Convert root node to ObjectNode to allow modifications
            ObjectNode cleanNode = (ObjectNode) rootNode;

            // Remove the "title" field
            cleanNode.remove("title");

            // Process "images": convert array to its count
            if (cleanNode.has("images") && cleanNode.get("images").isArray()) {
                int imageCount = cleanNode.get("images").size();
                cleanNode.put("images", imageCount);
            }

            // Build a composite key from user_id, asin, and timestamp
            String userId = cleanNode.hasNonNull("user_id") ? cleanNode.get("user_id").asText() : "";
            String asin = cleanNode.hasNonNull("asin") ? cleanNode.get("asin").asText() : "";
            String timestamp = cleanNode.hasNonNull("timestamp") ? cleanNode.get("timestamp").asText() : "";
            String compositeKey = userId + "-" + asin + "-" + timestamp;

            // Convert cleaned JSON back to a String
            String cleanedJson = objectMapper.writeValueAsString(cleanNode);

            // Print debug info before emitting
            System.out.println("DEBUG: Emitting -> KEY: " + compositeKey 
                               + " | VALUE: " + cleanedJson);

            // Emit the composite key and cleaned JSON record
            context.write(new Text(compositeKey), new Text(cleanedJson));

        } catch (Exception e) {
            // Print the parse error for debugging
            System.out.println("DEBUG: Parse error -> " + e.getMessage());
            // Skip this record or optionally count it
            // context.getCounter("CleanReviewsMapper", "PARSE_ERRORS").increment(1);
        }
    }
}
