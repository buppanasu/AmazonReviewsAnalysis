/**
 * Owner: Farhan
 *
 * Description:
 * This Reducer class for a Hadoop MapReduce job processes the output of the product description analysis. 
 * For each parent ASIN key, it emits the first value received (typically containing the description count 
 * and rating number). This is useful when there is only one entry per key or when the first entry is sufficient.
 */

package analysis.productdescriptionanalysis;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductDescriptionReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Simply emit the first value for each parent_asin.
        context.write(key, values.iterator().next());
    }
}
