package analysis.q3verifieduniquereviews;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VerifiedReviewsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int totalVerifiedReviews = 0;
    private int distinctVerifiedUsers = 0;

    @Override
    protected void reduce(Text userId, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sumForThisUser = 0;
        for (IntWritable val : values) {
            sumForThisUser += val.get();
        }
        // Add to global count
        totalVerifiedReviews += sumForThisUser;
        // Each reduce call is for a distinct user_id
        distinctVerifiedUsers++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Write out final aggregated results
        context.write(new Text("Total Verified Reviews"), new IntWritable(totalVerifiedReviews));
        context.write(new Text("Distinct Verified Users"), new IntWritable(distinctVerifiedUsers));
    }
}