package preprocessing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CleanProductMetaMapper extends Mapper<LongWritable, Text, Text, Text> {

    private ObjectMapper objectMapper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize Jackson ObjectMapper for JSON parsing
        objectMapper = new ObjectMapper();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String jsonLine = value.toString();

        try {
            // Parse the JSON line into a JsonNode
            JsonNode rootNode = objectMapper.readTree(jsonLine);

            // Convert to ObjectNode so we can remove fields
            if (!(rootNode instanceof ObjectNode)) {
                // If it's not an ObjectNode, skip it
                return;
            }
            ObjectNode cleanNode = (ObjectNode) rootNode;

            // Remove unwanted fields
            cleanNode.remove("images");
            cleanNode.remove("videos");
            cleanNode.remove("bought_together");
            cleanNode.remove("price");
            cleanNode.remove("features");
            cleanNode.remove("description");
            cleanNode.remove("categories");
            cleanNode.remove("subtitle");
            cleanNode.remove("author");
            cleanNode.remove("details");

            // Build a composite key to deduplicate by
            // Typically "asin" is the unique product ID
            // If "asin" is missing, fallback to "parent_asin"
            String asin = cleanNode.hasNonNull("asin") ? cleanNode.get("asin").asText() : "";
            String parentAsin = cleanNode.hasNonNull("parent_asin") ? cleanNode.get("parent_asin").asText() : "";

            // If asin is empty, use parent_asin, else use asin
            String dedupKey = asin.isEmpty() ? parentAsin : asin;
            if (dedupKey.isEmpty()) {
                // If neither asin nor parent_asin is present, skip
                return;
            }

            // Convert cleaned JSON back to a String
            String cleanedJson = objectMapper.writeValueAsString(cleanNode);

            // Emit the composite key and cleaned JSON record
            context.write(new Text(dedupKey), new Text(cleanedJson));

        } catch (Exception e) {
            // In case of parse error, skip or log
            // context.getCounter("CleanProductMetaMapper", "PARSE_ERRORS").increment(1);
        }
    }
}
