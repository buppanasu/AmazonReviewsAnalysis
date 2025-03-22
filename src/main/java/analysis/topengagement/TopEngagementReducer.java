package analysis.topengagement;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopEngagementReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // For each key (either a main category or "ALL"),
        // simply output each product record on its own.
        for (Text val : values) {
            // The output format will be: key \t productTitle,ratingNumber
            context.write(key, val);
            System.out.println("[REDUCER] Emitted: Key=" + key.toString() + " Value=" + val.toString());
        }
    }
}
