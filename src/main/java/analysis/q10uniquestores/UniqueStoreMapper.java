package analysis.q10uniquestores;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UniqueStoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text storeName = new Text();
    private IntWritable one = new IntWritable(1);
    private ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper

    @Override
    protected void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
    	// Split the line to get JSON content
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        String jsonLine = parts.length == 2 ? parts[1] : parts[0];
    	
        try {
        	// Parse JSON string into a JsonNode
            JsonNode root = objectMapper.readTree(jsonLine);
            JsonNode metadataNode = root.path("metadata");

            if (metadataNode != null && 
        		metadataNode.has("store") && 
        		!metadataNode.get("store").isNull()) {
            	
            	storeName.set(metadataNode.get("store").asText());
            	
            	context.write(storeName, one);
            	
            }

        } catch (Exception e) {
            // Log the error for debugging
            e.printStackTrace();
        }
    }
}
