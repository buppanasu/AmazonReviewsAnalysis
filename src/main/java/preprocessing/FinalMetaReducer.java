package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FinalMetaReducer extends Reducer<Text, Text, Text, Text> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Iterate over all values (JSON objects) for a given category (key)
        for (Text value : values) {
            // Directly emit the key (main_category) and the value (JSON string)
            context.write(key, value);
        }
    }
}
