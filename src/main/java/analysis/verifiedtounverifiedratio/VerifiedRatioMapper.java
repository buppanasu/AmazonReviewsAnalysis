package analysis.verifiedtounverifiedratio;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VerifiedRatioMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final IntWritable one = new IntWritable(1);
    private final Text verifiedKey = new Text();

    // Debug counters
    enum CountersEnum { RECORDS_PROCESSED, VALID_JSON, INVALID_JSON, VERIFIED_FOUND, VERIFIED_NOT_FOUND }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(CountersEnum.RECORDS_PROCESSED).increment(1);
        String line = value.toString();
        System.out.println("Processing record: " + line);

        // Split by tab if present
        String[] parts = line.split("\t", 2);
        if (parts.length == 2) {
            line = parts[1];
        } else {
            // If there is no tab, assume the whole line is JSON.
            System.out.println("No tab found; using entire line as JSON.");
        }

        try {
            JsonNode node = MAPPER.readTree(line);
            context.getCounter(CountersEnum.VALID_JSON).increment(1);
            if (node.has("verified_purchase")) {
                boolean verified = node.get("verified_purchase").asBoolean();
                verifiedKey.set(Boolean.toString(verified));
                context.write(verifiedKey, one);
                context.getCounter(CountersEnum.VERIFIED_FOUND).increment(1);
                System.out.println("Emitted key: " + verifiedKey.toString());
            } else {
                context.getCounter(CountersEnum.VERIFIED_NOT_FOUND).increment(1);
                System.out.println("Field 'verified_purchase' not found in record.");
            }
        } catch (Exception e) {
            context.getCounter(CountersEnum.INVALID_JSON).increment(1);
            System.err.println("Error parsing JSON: " + line);
            e.printStackTrace();
        }
    }
}
