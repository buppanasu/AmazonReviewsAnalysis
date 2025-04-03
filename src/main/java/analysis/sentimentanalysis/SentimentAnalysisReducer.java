package analysis.sentimentanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SentimentAnalysisReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            // Output format: parent_asin, main_category, timestamp, text
            context.write(key, value);
        }
    }
}
