/**
 * Owner: Travis Teo
 *
 * Description:
 * This mapper class is part of a Hadoop MapReduce job that preprocesses product reviews.
 * The CleanReviewsMapper performs the following steps:
 *   1. Initializes a Jackson ObjectMapper for JSON parsing during the setup phase.
 *   2. Reads each input line as a JSON string representing a product review.
 *   3. Parses the JSON string into a JsonNode and converts it into an ObjectNode for modification.
 *   4. Removes unnecessary fields such as "title" and "asin" (while preserving "asin" for key construction).
 *   5. Converts the "images" array into its count.
 *   6. Constructs a composite key using user_id, asin, and timestamp.
 *   7. Outputs the composite key along with the cleaned JSON string for further processing.
 * Debug statements are included to trace the processing of each record.
 */

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
        // Print the raw line for debugging
        System.out.println("DEBUG: Raw input line -> " + jsonLine);

        try {
            // Parse the JSON line into a JsonNode
            JsonNode rootNode = objectMapper.readTree(jsonLine);

            // Convert root node to ObjectNode so we can remove fields
            ObjectNode cleanNode = (ObjectNode) rootNode;

            // 1) Remove the "title" field
            cleanNode.remove("title");

            // 2) Remove the "asin" field (but we'll still use it for the composite key)
            String asin = "";
            if (cleanNode.hasNonNull("asin")) {
                asin = cleanNode.get("asin").asText();
                cleanNode.remove("asin");
            }

            // Convert "images" array to its count
            if (cleanNode.has("images") && cleanNode.get("images").isArray()) {
                int imageCount = cleanNode.get("images").size();
                cleanNode.put("images", imageCount);
            }

            // Build a composite key from user_id, asin, and timestamp
            String userId = cleanNode.hasNonNull("user_id") ? cleanNode.get("user_id").asText() : "";
            String timestamp = cleanNode.hasNonNull("timestamp") ? cleanNode.get("timestamp").asText() : "";
            String compositeKey = userId + "-" + asin + "-" + timestamp;

            // Convert cleaned JSON back to a String
            String cleanedJson = objectMapper.writeValueAsString(cleanNode);

            // Print debug info
            System.out.println("DEBUG: Emitting -> KEY: " + compositeKey + " | VALUE: " + cleanedJson);

            // Emit the composite key and cleaned JSON record
            context.write(new Text(compositeKey), new Text(cleanedJson));

        } catch (Exception e) {
            // Print parse error for debugging
            System.out.println("DEBUG: Parse error -> " + e.getMessage());
            // Skip this record
        }
    }
}
