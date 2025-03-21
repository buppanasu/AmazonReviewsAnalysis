package final_merge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class FinalMetaJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("DEBUG: Received input - " + value.toString());  // Add this line to confirm data input

        try {
            // Clean the string by removing everything before the first '{'
            String cleanValue = value.toString();
            int jsonStartIndex = cleanValue.indexOf("{");

            if (jsonStartIndex != -1) {
                cleanValue = cleanValue.substring(jsonStartIndex);  // Extract the JSON part

                // Parse the cleaned JSON string
                JsonNode metadata = mapper.readTree(cleanValue);

                // Get the main_category from the key
                String mainCategory = value.toString().split("\t")[0].trim();  // Assuming key and value are separated by a tab

                // Convert the metadata JsonNode to an ObjectNode (to modify it)
                if (metadata instanceof ObjectNode) {
                    // Add the "main_category" field to the metadata
                    ((ObjectNode) metadata).put("main_category", mainCategory);
                }

                String parentAsin = metadata.has("parent_asin") ? metadata.get("parent_asin").asText() : null;

                if (parentAsin != null && !parentAsin.isEmpty()) {
                    // Emit the "metadata" tag along with the cleaned and updated metadata
                    System.out.println("DEBUG: Emitting key: " + parentAsin + " value: " + metadata.toString());  // Debug output
                    context.write(new Text(parentAsin), new Text("metadata\t" + metadata.toString()));
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing metadata: " + e.getMessage());
        }
    }
}
