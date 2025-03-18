package analysis.q5reviewedproducts;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReviewedProductsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private final static IntWritable ONE = new IntWritable(1);
    private Text productKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            // Each line is a JSON record from your cleaned reviews.
            JsonNode rootNode = objectMapper.readTree(value.toString());
            JsonNode parentAsinNode = rootNode.get("parent_asin");
            if (parentAsinNode != null && !parentAsinNode.isNull()) {
                productKey.set(parentAsinNode.asText());
                context.write(productKey, ONE);
            }
        } catch (Exception e) {
            System.err.println("Parse error in ReviewedProductsMapper: " + e.getMessage());
        }
    }
}