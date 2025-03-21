package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

public class CleanAndCategorisedMetaMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    // Define a mapping for main_category to customized categories
    private static final Map<String, String> categoryMapping = new HashMap<>();
    
    static {
        // Define the category mapping
        categoryMapping.put("Video Games", "Video Games");
        categoryMapping.put("Computers", "Computers");
        categoryMapping.put("All Electronics", "All Electronics");
        categoryMapping.put("Toys & Games", "Toys & Games");
        categoryMapping.put("Baby", "Toys & Games");
        categoryMapping.put("Musical Instruments", "Toys & Games");
        categoryMapping.put("Software", "Software");
        categoryMapping.put("Collectible Coins", "Software");
        categoryMapping.put("Buy a Kindle", "Media");
        categoryMapping.put("Movies & TV", "Media");
        categoryMapping.put("Books", "Media");
        categoryMapping.put("Digital Music", "Media");
        categoryMapping.put("Audible Audiobooks", "Media");
        categoryMapping.put("Cell Phones & Accessories", "Cell Phone & Camera w. Accessories");
        categoryMapping.put("Camera & Photo", "Cell Phone & Camera w. Accessories");
        categoryMapping.put("Portable Audio & Accessories", "Cell Phone & Camera w. Accessories");
        categoryMapping.put("Sports & Outdoors", "Sports & Health");
        categoryMapping.put("All Beauty", "Sports & Health");
        categoryMapping.put("Health & Personal Care", "Sports & Health");
        categoryMapping.put("Pet Supplies", "Sports & Health");
        categoryMapping.put("Industrial & Scientific", "Daily Gadgets");
        categoryMapping.put("Amazon Home", "Daily Gadgets");
        categoryMapping.put("Home Audio & Theater", "Daily Gadgets");
        categoryMapping.put("Tools & Home Improvement", "Daily Gadgets");
        categoryMapping.put("Office Products", "Daily Gadgets");
        categoryMapping.put("Automotive", "Daily Gadgets");
        categoryMapping.put("Car Electronics", "Daily Gadgets");
        categoryMapping.put("GPS & Navigation", "Daily Gadgets");
        categoryMapping.put("Appliances", "Daily Gadgets");
        categoryMapping.put("Amazon Devices", "Daily Gadgets");
        categoryMapping.put("Others", "Others");
        categoryMapping.put("Handmade", "Others");
        categoryMapping.put("Gift Cards", "Others");
        categoryMapping.put("Grocery", "Others");
        categoryMapping.put("AMAZON FASHION", "Others");
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JsonNode product = mapper.readTree(value.toString());
            
            // Extract required fields
            String mainCategory = product.has("main_category") ? product.get("main_category").asText("") : "";
            String parentAsin = product.has("parent_asin") ? product.get("parent_asin").asText("") : "";
            int ratingNumber = product.has("rating_number") ? product.get("rating_number").asInt(0) : 0;
            float averageRating = product.has("average_rating") ? product.get("average_rating").floatValue() : 0.0f;
            
            // Apply the category mapping
            if (categoryMapping.containsKey(mainCategory)) {
                mainCategory = categoryMapping.get(mainCategory); // Map to the new category
            } else {
                mainCategory = "Others"; // Default to "Others" if not in the mapping
            }
            
            // Check if main_category is null or blank, assign to "Others"
            if (mainCategory == null || mainCategory.trim().isEmpty()) {
                mainCategory = "Others";
            }
            
            // Skip if parent_asin is missing
            if (parentAsin == null || parentAsin.trim().isEmpty()) {
                return;
            }
            
            // Create output value as JSON
            ObjectNode outputValue = mapper.createObjectNode();
            outputValue.put("parent_asin", parentAsin);
            outputValue.put("rating_number", ratingNumber);
            outputValue.put("average_rating", averageRating);
            
            // Emit main_category as key and JSON object as value
            context.write(new Text(mainCategory), new Text(outputValue.toString()));
            
        } catch (Exception e) {
            // Log error and continue processing
            System.err.println("Error processing record: " + e.getMessage());
        }
    }
}
