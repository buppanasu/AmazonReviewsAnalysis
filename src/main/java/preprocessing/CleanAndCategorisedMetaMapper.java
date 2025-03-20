package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CleanAndCategorisedMetaMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JsonNode product = mapper.readTree(value.toString());
            
            // Extract required fields
            String mainCategory = product.has("main_category") ? product.get("main_category").asText("") : "";
            String parentAsin = product.has("parent_asin") ? product.get("parent_asin").asText("") : "";
            int ratingNumber = product.has("rating_number") ? product.get("rating_number").asInt(0) : 0;
            float averageRating = product.has("average_rating") ? product.get("average_rating").floatValue() : 0.0f;
            
            // Check if main_category is null or blank, assign to "Others"
            if (mainCategory == null || mainCategory.trim().isEmpty()) {
                mainCategory = "Others";
            }
            
            // Skip if parent_asin is missing
            if (parentAsin == null || parentAsin.trim().isEmpty()) {
                return;
            }
            
            // Create output value as JSON
            ObjectNode outputValue = mapper.createObjectNode();
            outputValue.put("parent_asin", parentAsin);
            outputValue.put("rating_number", ratingNumber);
            outputValue.put("average_rating", averageRating);
            
            // Emit main_category as key and JSON object as value
            context.write(new Text(mainCategory), new Text(outputValue.toString()));
            
        } catch (Exception e) {
            // Log error and continue processing
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
