package preprocessing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class CleanAndCategorisedMetaReducer extends Reducer<Text, Text, Text, Text> {
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> parentAsins = new HashSet<>();
        long totalRatingNumber = 0;
        float sumAverageRating = 0;
        
        // Process all values for this main_category
        for (Text value : values) {
            JsonNode productData = mapper.readTree(value.toString());
            String parentAsin = productData.get("parent_asin").asText();
            int ratingNumber = productData.get("rating_number").asInt();
            float averageRating = productData.get("average_rating").floatValue();
            
            // Add parent_asin to set (removes duplicates)
            parentAsins.add(parentAsin);
            
            // Sum rating_number across all products
            totalRatingNumber += ratingNumber;
            
            // Sum average_rating for later calculation
            sumAverageRating += averageRating;
        }
        
        // Calculate actual average of average_ratings
        float categoryAverageRating = parentAsins.size() > 0 ? sumAverageRating / parentAsins.size() : 0;
        
        // Create JSON output
        ObjectNode result = mapper.createObjectNode();
        result.put("count", parentAsins.size());
        result.put("total_rating_number", totalRatingNumber);
        result.put("average_rating", categoryAverageRating);
        
        // Create array of parent_asins
        ArrayNode asinArray = result.putArray("parent_asin");
        for (String asin : parentAsins) {
            asinArray.add(asin);
        }
        
        // Write result
        context.write(key, new Text(result.toString()));
    }
}
