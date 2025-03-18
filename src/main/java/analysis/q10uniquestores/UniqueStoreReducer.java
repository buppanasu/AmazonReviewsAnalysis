package analysis.q10uniquestores;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap;
import java.util.ArrayList;

public class UniqueStoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private List<Map.Entry<String, Integer>> sumList = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        // Store the sum and key as an entry in a list
        sumList.add(new AbstractMap.SimpleEntry<>(key.toString(), sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort the list by sum in descending order
        sumList.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));

        // Write the sorted values to context
        for (Map.Entry<String, Integer> entry : sumList) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}

