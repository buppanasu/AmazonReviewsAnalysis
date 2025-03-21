package final_merge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class FinalReviewJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // Clean the string by removing the review ID, which is before the first '{'
            String cleanValue = value.toString();
            int jsonStartIndex = cleanValue.indexOf("{");

            if (jsonStartIndex != -1) {
                cleanValue = cleanValue.substring(jsonStartIndex);  // Extract the JSON part

                // Parse the cleaned JSON string
                JsonNode review = mapper.readTree(cleanValue);
                String parentAsin = review.has("parent_asin") ? review.get("parent_asin").asText() : null;

                if (parentAsin != null && !parentAsin.isEmpty()) {
                    // Emit "review" tag along with the cleaned review data
//                	System.out.println("Review Mapper <key:"+parentAsin+"> <value:"+review+">");
                    context.write(new Text(parentAsin), new Text("review\t" + cleanValue));
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing review: " + e.getMessage());
        }
    }
}