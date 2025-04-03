/**
 * Owner: Alan
 *
 * Description:
 * This Mapper class for a Hadoop MapReduce job extracts sentiment-related fields from 
 * JSON-formatted Amazon product reviews. For each input line, it parses the JSON to retrieve 
 * the main category, review timestamp, and review text, using the parent ASIN as the key. 
 * It emits the parent ASIN and a tab-separated string of the extracted fields as the output.
 */

package analysis.sentimentanalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t", 2);

        if (parts.length == 2) {
            String parentAsin = parts[0];
            String jsonString = parts[1];

            try {
                JsonNode root = objectMapper.readTree(jsonString);

                // Extracting the main_category
                String mainCategory = root.path("metadata").path("main_category").asText();

                // Extracting the timestamp
                String timestamp = root.path("review").path("timestamp").asText();

                // Extracting the text
                String reviewText = root.path("review").path("text").asText();

                // Output key is parent_asin, value is concatenated fields
                context.write(new Text(parentAsin), new Text(mainCategory + "\t" + timestamp + "\t" + reviewText));
            } catch (Exception e) {
                // Handle exception (e.g., malformed JSON)
                System.err.println("Error processing line: " + e.getMessage());
            }
        }
    }
}
