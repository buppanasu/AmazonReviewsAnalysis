package analysis.q1distinctitems;


import java.io.IOException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctItemMapper extends Mapper<Object, Text, Text, Text> {
    
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Get the raw line from input
        String line = value.toString();
        
        // Split the line by tab (if there is a key before the JSON)
        String[] parts = line.split("\t", 2);
        String jsonLine = parts.length == 2 ? parts[1] : parts[0];
        
        // Optional: Print debug info for verification
        System.out.println("DEBUG: Extracted JSON -> " + jsonLine);
        
        try {
            // Parse the JSON line
            JsonNode root = objectMapper.readTree(jsonLine);
            
            // If the JSON object has a non-null "parent_asin", emit it as the key
            if (root.hasNonNull("parent_asin")) {
                String parentAsin = root.get("parent_asin").asText();
                context.write(new Text(parentAsin), new Text(""));
            }
        } catch (Exception e) {
            // Print the error message along with the original line for debugging
            System.err.println("DEBUG: Parse error -> " + e.getMessage() + " for line: " + line);
        }
    }
}