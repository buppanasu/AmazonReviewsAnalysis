package analysis.split.to.maincategories;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SplitPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        // Use the main_category (key) to determine which partition the record goes to.
        String mainCategory = key.toString();
        
        // We assume you have up to 10 categories
        return Math.abs(mainCategory.hashCode()) % numPartitions;  // Ensures a good distribution across reducers
    }
}
