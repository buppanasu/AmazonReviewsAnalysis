package preprocessing;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DedupReducer extends Reducer<Text, Text, Text, Text> {
    // A counter to assign sequential row numbers
    private long counter = 1;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // For each unique key, output only one record
        // Use the counter as the new key
        for (Text val : values) {
            context.write(new Text(String.valueOf(counter++)), val);
            break; // Only output one record per unique key
        }
    }
}