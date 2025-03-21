package final_merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FinalJoinReducer extends Reducer<Text, Text, Text, Text> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String metadata = null;
        List<String> reviews = new ArrayList<>();

        for (Text value : values) {
            String[] parts = value.toString().split("\t", 2);
            if (parts[0].equals("metadata")) {
                metadata = parts[1];
            } else if (parts[0].equals("review")) {
                reviews.add(parts[1]);
            }
        }

        if (metadata != null) {
            JsonNode metadataJson = mapper.readTree(metadata);

            for (String reviewStr : reviews) {
                JsonNode reviewJson = mapper.readTree(reviewStr);
                ObjectNode combined = mapper.createObjectNode();
                combined.set("metadata", metadataJson);
                combined.set("review", reviewJson);
                System.out.println("Reducer <key:"+key+"> <value:"+combined+">");
                context.write(key, new Text(combined.toString()));  // Ensure this line is being called
            }
        }
    }
}
