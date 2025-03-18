package analysis.q4userwithmostreviews;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostReviewsUserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int maxCount = 0;
    private Text maxUser = new Text("NoUser");

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Compare local sum to global max
        if (sum > maxCount) {
            maxCount = sum;
            maxUser.set(key);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // After processing all user_ids, output the single user with the max reviews
        context.write(maxUser, new IntWritable(maxCount));
    }
}