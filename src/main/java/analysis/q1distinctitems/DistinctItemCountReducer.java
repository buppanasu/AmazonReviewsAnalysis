package analysis.q1distinctitems;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctItemCountReducer extends Reducer<Text, Text, Text, IntWritable> {

    private int distinctCount = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Each key is a unique parent_asin, so increment the count.
        distinctCount++;
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Write out the total count after processing all keys.
        context.write(new Text("Total Unique Items:"), new IntWritable(distinctCount));
    }
}