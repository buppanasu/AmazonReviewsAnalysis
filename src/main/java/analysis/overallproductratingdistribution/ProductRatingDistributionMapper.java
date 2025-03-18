package analysis.overallproductratingdistribution;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ProductRatingDistributionMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final IntWritable one = new IntWritable(1);
    private final IntWritable ratingKey = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString();
        
        // Look for the tab character separating the index and JSON.
        int tabIndex = line.indexOf("\t");
        if (tabIndex < 0) {
            // If no tab is found, skip the line.
            return;
        }
        
        // Extract the JSON part after the tab.
        String jsonPart = line.substring(tabIndex + 1).trim();
        
        // Try to parse the JSON.
        try {
            JsonNode node = MAPPER.readTree(jsonPart);
            if (node.has("rating")) {
                // Convert the rating (float/double) to an integer key.
                int rating = (int) node.get("rating").asDouble();
                ratingKey.set(rating);
                context.write(ratingKey, one);
            }
        } catch (Exception e) {
            // Log the error and skip the line.
            System.err.println("Failed to parse JSON: " + jsonPart);
        }
    }
}
