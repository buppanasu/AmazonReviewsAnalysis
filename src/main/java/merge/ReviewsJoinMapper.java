package merge;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReviewsJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String jsonLine = value.toString();
        // Debug print the received line
        System.out.println("DEBUG ReviewsJoinMapper: Received line: " + jsonLine);
        try {
            JsonNode node = objectMapper.readTree(jsonLine);
            // Extract the join key from the "parent_asin" field
            String parentAsin = "";
            if (node.hasNonNull("parent_asin")) {
                parentAsin = node.get("parent_asin").asText();
            }
            if (!parentAsin.trim().isEmpty()) {
                String outputVal = "R|" + jsonLine;
                context.write(new Text(parentAsin), new Text(outputVal));
                System.out.println("DEBUG ReviewsJoinMapper: Emitted key: " + parentAsin + " | value: " + outputVal);
            } else {
                System.out.println("DEBUG ReviewsJoinMapper: Skipping record - 'parent_asin' is empty.");
            }
        } catch (Exception e) {
            System.err.println("DEBUG ReviewsJoinMapper: Error parsing record: " + e.getMessage());
        }
    }
}
