package analysis.q4userwithmostreviews;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MostReviewsUserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private ObjectMapper objectMapper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        objectMapper = new ObjectMapper();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            // Parse the JSON line
            JsonNode rootNode = objectMapper.readTree(value.toString());
            
            // Extract the user_id
            if (rootNode.hasNonNull("user_id")) {
                String userId = rootNode.get("user_id").asText();
                // Emit (user_id, 1) for each review
                context.write(new Text(userId), ONE);
            }
        } catch (Exception e) {
            // In case of any parse errors, skip
            // context.getCounter("MAP_ERRORS", "PARSE_ERROR").increment(1);
        }
    }
}