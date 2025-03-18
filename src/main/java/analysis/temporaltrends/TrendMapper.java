package analysis.temporaltrends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TrendMapper extends Mapper<Object, Text, Text, Text> {

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
                String parentAsin = jsonNode.get("parent_asin").asText();
                long timestamp = jsonNode.get("timestamp").asLong();
                double rating = jsonNode.get("rating").asDouble();

                // Format the date to "YYYY-MM"
                String date = new java.text.SimpleDateFormat("yyyy-MM")
                        .format(new java.util.Date(timestamp));

                // Construct the key: "<parent_asin, YYYY-MM>"
                outputKey.set(parentAsin + "," + date);

                // Construct the value: "<rating, 1>"
                outputValue.set(rating + ",1");

                // Emit the key-value pair
                context.write(outputKey, outputValue);

            } catch (Exception e) {
                // Skip invalid entries (if any)
                e.printStackTrace();
            }
        }
    }
}
