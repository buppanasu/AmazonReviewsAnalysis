package analysis.reviewanomaly;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReviewAnomalyMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private ObjectMapper objectMapper = new ObjectMapper();  // Jackson ObjectMapper

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line by tab to get the review
        String line = value.toString();
        String[] columns = line.split("\t");

        // Extract the JSON part and parse using Jackson
        if (columns.length > 1) {
            try {
                JsonNode jsonNode = objectMapper.readTree(columns[1]);
                String userId = jsonNode.get("user_id").asText();
                boolean verifiedPurchase = jsonNode.get("verified_purchase").asBoolean();
                long timestamp = jsonNode.get("timestamp").asLong();

                // Only emit data if verified_purchase == false
//                if (!verifiedPurchase) {
                    // Emit key-value pair with user_id and timestamp
                    outputKey.set(userId);
                    outputValue.set("1," + timestamp);  // "1" for count, and the timestamp
                    context.write(outputKey, outputValue);
//                }
            } catch (Exception e) {
                // Skip invalid entries (if any)
                e.printStackTrace();
            }
        }
    }

}
