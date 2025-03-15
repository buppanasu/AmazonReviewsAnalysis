package merge;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetaJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String jsonLine = value.toString();
        try {
            JsonNode node = objectMapper.readTree(jsonLine);
            // Extract the join key from the "parent_asin" field
            String parentAsin = "";
            if (node.hasNonNull("parent_asin")) {
                parentAsin = node.get("parent_asin").asText();
            }
            if (!parentAsin.trim().isEmpty()) {
                String outputVal = "M|" + jsonLine;
                context.write(new Text(parentAsin), new Text(outputVal));
                System.out.println("DEBUG MetaJoinMapper: Emitted key: " + parentAsin + " | value: " + outputVal);
            } else {
                System.out.println("DEBUG MetaJoinMapper: Skipping record - 'parent_asin' is empty.");
            }
        } catch (Exception e) {
            System.err.println("DEBUG MetaJoinMapper: Error parsing record: " + e.getMessage());
        }
    }
}
