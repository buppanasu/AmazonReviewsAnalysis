package analysis.q7top10highestreviews;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class top10reviewsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. Split off any leading key + tab
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        String jsonLine = parts.length == 2 ? parts[1] : parts[0];

        try {
            // Parse the JSON string into a JsonNode
            JsonNode root = objectMapper.readTree(jsonLine);

            // Extract the rating_number from the metadata
            JsonNode metadata = root.path("metadata");
            
            if (metadata.has("rating_number") && metadata.get("rating_number").isInt()) {
            	
                int ratingNumber = metadata.get("rating_number").asInt();

                // Write the rating_number and the original JSON string to the context
                context.write(new IntWritable(ratingNumber), new Text(jsonLine));
            }
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
