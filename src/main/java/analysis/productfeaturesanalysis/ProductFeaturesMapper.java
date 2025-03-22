package analysis.productfeaturesanalysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProductFeaturesMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private ObjectMapper objectMapper = new ObjectMapper();
    private Text outKey = new Text();
    private Text outValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Each line is expected to be: <Category>\t<JSON string>
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        if (parts.length < 2) {
            return; // skip malformed lines
        }
        
        String jsonStr = parts[1].trim();
        try {
            JsonNode root = objectMapper.readTree(jsonStr);
            
            // Extract parent_asin
            if (!root.hasNonNull("parent_asin")) {
                return;
            }
            String parentAsin = root.get("parent_asin").asText();
            
            // Extract the features field as text.
            if (!root.has("features")) {
                return;
            }
            String featuresStr = root.get("features").asText();
            JsonNode featuresNode = objectMapper.readTree(featuresStr);
            int featureCount = 0;
            if (featuresNode.isArray()) {
                featureCount = featuresNode.size();
            }
            
            // Extract the rating number. (Assume it's stored in "rating_number")
            String ratingNumber = "0";  // default value
            if (root.hasNonNull("rating_number")) {
                ratingNumber = root.get("rating_number").asText();
            }
            
            // Emit key as parent_asin and value as "featureCount,ratingNumber"
            outKey.set(parentAsin);
            outValue.set(featureCount + "," + ratingNumber);
            context.write(outKey, outValue);
        } catch (Exception e) {
            System.err.println("ProductFeaturesMapper parse error: " + e.getMessage() + " | Line: " + line);
        }
    }
}
