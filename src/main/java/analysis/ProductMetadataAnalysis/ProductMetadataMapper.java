package analysis.ProductMetadataAnalysis;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProductMetadataMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Each line is from cleaned_metadata.csv
        // The format is:
        // <Category>    <JSON string>
        // We assume that fields are separated by a tab.
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        if (parts.length < 2) {
            return; // Malformed line, skip it.
        }
        
        // parts[0] is the category (e.g., "All Electronics")
        // parts[1] is the JSON string with product details.
        String jsonStr = parts[1].trim();
        try {
            JsonNode root = objectMapper.readTree(jsonStr);
            
            // Use the "parent_asin" as the product identifier.
            if (!root.hasNonNull("parent_asin")) {
                return;
            }
            String productId = root.get("parent_asin").asText();
            
            // Extract fields: image_count and rating_number.
            int imageCount = root.hasNonNull("image_count") ? root.get("image_count").asInt() : 0;
            int ratingNumber = root.hasNonNull("rating_number") ? root.get("rating_number").asInt() : 0;
            int videoCount = root.hasNonNull("video_count") ? root.get("video_count").asInt() : 0;
            
            // We can format the value as a CSV-like string or as a JSON array.
            // For this example, we use a CSV string: image_count, rating_number, video_count.
            String outputValue = imageCount + "," + ratingNumber + "," + videoCount;
            
            // Emit key: productId, value: outputValue.
            outKey.set(productId);
            outValue.set(outputValue);
            context.write(outKey, outValue);
            
        } catch (Exception e) {
            System.err.println("ProductMetadataMapper parse error: " + e.getMessage() + " | Line: " + line);
        }
    }
}
