/**
 * Owner: Yu Rui
 *
 * Description:
 * This Mapper class for a Hadoop MapReduce job identifies high-engagement products by parsing 
 * JSON-formatted review metadata. It extracts the main category, product title, and number of ratings 
 * (rating_number) for each product. It emits two records per product: one keyed by the main category 
 * and another under a generic "ALL" key for global ranking. The output value is a comma-separated 
 * string of product title and rating count.
 */

package analysis.topengagement;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TopEngagementMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Text OVERALL_KEY = new Text("ALL");
    private Text outKey = new Text();
    private Text outValue = new Text();
    private ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Each line is expected to have: <MainCategory><tab><JSON record>
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        
        String[] tokens = line.split("\t", 2);
        if (tokens.length < 2) {
            System.err.println("[MAPPER] Skipping invalid record (not enough tokens): " + line);
            return;
        }
        
        String mainCategory = tokens[0];
        String jsonStr = tokens[1];
        String productTitle = "unknown";
        int ratingNumber = 0;
        
        try {
            JsonNode root = jsonMapper.readTree(jsonStr);
            JsonNode titleNode = root.get("title");
            if (titleNode != null) {
                productTitle = titleNode.asText();
            }
            JsonNode ratingNode = root.get("rating_number");
            if (ratingNode != null) {
                ratingNumber = ratingNode.asInt();
            }
        } catch (Exception e) {
            System.err.println("[MAPPER] JSON parse error for record: " + line);
            e.printStackTrace();
            return;
        }
        
        // Prepare a value string: productTitle,ratingNumber
        String outValStr = productTitle + "," + ratingNumber;
        outValue.set(outValStr);
        
        // Emit keyed by the main category.
        outKey.set(mainCategory);
        context.write(outKey, outValue);
        System.out.println("[MAPPER] Emitted: Key=" + outKey.toString() + " Value=" + outValue.toString());
        
        // Also emit under the "ALL" key for overall top-engagement.
        context.write(OVERALL_KEY, outValue);
        System.out.println("[MAPPER] Emitted: Key=ALL Value=" + outValue.toString());
    }
}
