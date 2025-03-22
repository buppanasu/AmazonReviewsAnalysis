package analysis.descriptiontokenanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DescriptionTokenMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Set<String> stopwords = new HashSet<>();
    private ObjectMapper jsonMapper = new ObjectMapper();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        // Get stopwords file path from configuration.
        String stopwordsPath = conf.get("stopwords.path");
        if (stopwordsPath != null) {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(stopwordsPath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = br.readLine()) != null) {
                // Add each trimmed, lower-case stopword.
                stopwords.add(line.trim().toLowerCase());
            }
            br.close();
        }
    }
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Expect each line to be tab-separated: main_category \t JSON
        String[] parts = value.toString().split("\t", 2);
        if (parts.length < 2) {
            return;
        }
        String mainCategory = parts[0];
        String jsonStr = parts[1];
        
        try {
            // Parse JSON string into a Map.
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> jsonMap = jsonMapper.readValue(jsonStr, java.util.Map.class);
            Object descriptionObj = jsonMap.get("description");
            Object ratingObj = jsonMap.get("rating_number");
            if (ratingObj == null) {
                return;
            }
            int ratingNumber;
            try {
                ratingNumber = Integer.parseInt(ratingObj.toString());
            } catch (NumberFormatException e) {
                return;
            }
            // We expect description to be a JSON array string.
            if (descriptionObj != null && descriptionObj instanceof String) {
                String desc = (String) descriptionObj;
                // Parse the description string as an array of strings.
                String[] descTokens = jsonMapper.readValue(desc, String[].class);
                // Use a set to ensure that for each product, a token is only emitted once.
                Set<String> uniqueWords = new HashSet<>();
                for (String token : descTokens) {
                    // Split token into words using non-alphabetic characters as delimiters.
                    String[] words = token.split("[^a-zA-Z]+");
                    for (String word : words) {
                        if (word.isEmpty()) continue;
                        String lower = word.toLowerCase();
                        if (stopwords.contains(lower)) continue;
                        uniqueWords.add(lower);
                    }
                }
                // Emit (mainCategory + "\t" + word, ratingNumber) for each unique word.
                for (String word : uniqueWords) {
                    context.write(new Text(mainCategory + "\t" + word), new IntWritable(ratingNumber));
                }
            }
        } catch(Exception e) {
            System.err.println("Error processing line: " + e.getMessage());
        }
    }
}
