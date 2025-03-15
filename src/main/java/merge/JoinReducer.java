package merge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    private ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        ObjectNode metadataNode = null;
        List<JsonNode> reviewNodes = new ArrayList<>();
        
        // Process each value for the join key
        for (Text val : values) {
            String record = val.toString();
            if (record.startsWith("M|")) {
                // Remove the tag and parse metadata JSON
                String metaJson = record.substring(2);
                metadataNode = (ObjectNode) objectMapper.readTree(metaJson);
            } else if (record.startsWith("R|")) {
                // Remove the tag and parse review JSON
                String reviewJson = record.substring(2);
                JsonNode reviewNode = objectMapper.readTree(reviewJson);
                // Remove redundant parent_asin from review (if present)
                if (reviewNode.has("parent_asin")) {
                    ((ObjectNode) reviewNode).remove("parent_asin");
                }
                reviewNodes.add(reviewNode);
            }
        }
        
        // Build the output JSON object
        ObjectNode joinResult = objectMapper.createObjectNode();
        // Set the top-level parent_asin from the join key
        joinResult.put("parent_asin", key.toString());
        
        if (metadataNode != null) {
            // Remove the redundant parent_asin from metadata
            metadataNode.remove("parent_asin");
            joinResult.set("metadata", metadataNode);
        } else {
            joinResult.putNull("metadata");
        }
        
        // Create a reviews array and add all review nodes
        ArrayNode reviewsArray = objectMapper.createArrayNode();
        for (JsonNode review : reviewNodes) {
            reviewsArray.add(review);
        }
        joinResult.set("reviews", reviewsArray);
        
        // Write the result
        context.write(key, new Text(joinResult.toString()));
        System.out.println("DEBUG JoinReducer: Emitted record for key " + key.toString() 
                           + " -> " + joinResult.toString());
    }
}
