package WeightedAverageRatingAndVariance;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WARAVMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Text outKey = new Text();
    private final Text outValue = new Text();

    // Debug counters for verification
    enum CountersEnum { RECORDS_PROCESSED, VALID_JSON, INVALID_JSON, RATING_FOUND, RATING_NOT_FOUND }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(CountersEnum.RECORDS_PROCESSED).increment(1);

        // The input line is formatted as: recordId<TAB>JSON string
        String line = value.toString();
        String[] parts = line.split("\t", 2);
        if (parts.length == 2) {
            line = parts[1];
        }

        System.out.println("Processing record: " + line);

        try {
            JsonNode node = MAPPER.readTree(line);
            context.getCounter(CountersEnum.VALID_JSON).increment(1);

            // Check for "parent_asin" (used as product id) and "rating"
            if (node.has("parent_asin") && node.has("rating")) {
                String productId = node.get("parent_asin").asText();
                double rating = node.get("rating").asDouble();

                // Compute rating squared
                double ratingSquared = rating * rating;

                // Emit: key = productId, value = "rating,ratingSquared,1"
                outKey.set(productId);
                outValue.set(rating + "," + ratingSquared + ",1");
                context.write(outKey, outValue);
                context.getCounter(CountersEnum.RATING_FOUND).increment(1);
                System.out.println("Emitted key: " + outKey.toString() + " with value: " + outValue.toString());
            } else {
                context.getCounter(CountersEnum.RATING_NOT_FOUND).increment(1);
                System.out.println("Required fields not found in record.");
            }

        } catch (Exception e) {
            context.getCounter(CountersEnum.INVALID_JSON).increment(1);
            System.err.println("Error parsing JSON: " + line);
            e.printStackTrace();
        }
    }
}
