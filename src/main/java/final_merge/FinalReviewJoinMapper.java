/**
 * Owner: Travis Teo
 *
 * Description:
 * This mapper class is part of a Hadoop MapReduce job that processes and prepares
 * product metadata for a join operation with product reviews. It cleans the input by
 * stripping any extraneous characters before the JSON starts, parses the JSON to an ObjectNode,
 * and injects the "main_category" derived from the input record. Finally, it extracts the
 * parent_asin to use as the key, emitting the tagged metadata (prefixed with "metadata")
 * for downstream processing. Debug statements are included to facilitate tracing of the data flow.
 */

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