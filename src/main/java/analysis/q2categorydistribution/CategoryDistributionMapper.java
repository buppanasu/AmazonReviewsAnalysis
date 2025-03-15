package analysis.q2categorydistribution;


import java.io.IOException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoryDistributionMapper extends Mapper<Object, Text, Text, IntWritable> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private final static IntWritable one = new IntWritable(1);
    private Text category = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        // In case the input line starts with a key and a tab, split it out.
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        String jsonLine = parts.length == 2 ? parts[1] : parts[0];

        try {
            JsonNode root = objectMapper.readTree(jsonLine);
            JsonNode metadata = root.get("metadata");

            if (metadata != null && metadata.has("main_category") && !metadata.get("main_category").isNull()) {
                category.set(metadata.get("main_category").asText());
            } else {
                category.set("UNKNOWN");
            }

            // Emit the category and a count of 1
            context.write(category, one);

        } catch (Exception e) {
            System.err.println("DEBUG: Parse error in CategoryDistributionMapper: " + e.getMessage());
        }
    }
}