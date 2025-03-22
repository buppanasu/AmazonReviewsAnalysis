package analysis.productdescriptionanalysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProductDescriptionMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Each input line is assumed to be: <Category>\t<JSON string>
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        if (parts.length < 2) {
            return; // skip malformed lines
        }
        
        String jsonStr = parts[1].trim();
        try {
            JsonNode root = objectMapper.readTree(jsonStr);
            
            // Extract parent_asin; if missing, skip this record.
            if (!root.hasNonNull("parent_asin")) {
                return;
            }
            String parentAsin = root.get("parent_asin").asText();
            
            // Extract the "description" field.
            if (!root.has("description")) {
                return;
            }
            String descriptionStr = root.get("description").asText();
            // Parse the description string into a JSON array.
            int descriptionCount = 0;
            try {
                JsonNode descNode = objectMapper.readTree(descriptionStr);
                if (descNode.isArray()) {
                    descriptionCount = descNode.size();
                }
            } catch (Exception e) {
                // If parsing fails, assume count is 0.
                descriptionCount = 0;
            }
            
            // Extract the rating number (as text) from "rating_number".
            String ratingNumber = "0";  // default value
            if (root.hasNonNull("rating_number")) {
                ratingNumber = root.get("rating_number").asText();
            }
            
            // Emit the output: key = parent_asin, value = "descriptionCount,ratingNumber"
            outKey.set(parentAsin);
            outValue.set(descriptionCount + "," + ratingNumber);
            context.write(outKey, outValue);
            
        } catch (Exception e) {
            System.err.println("ProductDescriptionMapper parse error: " + e.getMessage() + " | Line: " + line);
        }
    }
}
