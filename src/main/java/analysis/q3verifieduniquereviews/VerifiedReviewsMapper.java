package analysis.q3verifieduniquereviews;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VerifiedReviewsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1. Split off any leading key + tab
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        String jsonLine = parts.length == 2 ? parts[1] : parts[0];

        try {
            // 2. Parse the JSON
            JsonNode root = objectMapper.readTree(jsonLine);

            // 3. The "reviews" array
            JsonNode reviewsArray = root.get("reviews");
            if (reviewsArray != null && reviewsArray.isArray()) {
                for (JsonNode review : reviewsArray) {
                    // Check if verified
                    JsonNode verifiedNode = review.get("verified_purchase");
                    if (verifiedNode != null && verifiedNode.asBoolean()) {
                        // Extract user_id
                        JsonNode userIdNode = review.get("user_id");
                        if (userIdNode != null && !userIdNode.asText().isEmpty()) {
                            String userId = userIdNode.asText();
                            // 4. Emit (user_id, 1)
                            context.write(new Text(userId), one);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("DEBUG: Parse error in VerifiedReviewsMapper: " + e.getMessage());
        }
    }
}