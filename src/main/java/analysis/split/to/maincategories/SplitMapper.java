package analysis.split.to.maincategories;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;
import java.util.Calendar;

public class SplitMapper  extends Mapper<Object, Text, Text, Text> {

	@Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        // Assuming input format: item_id, main_category, timestamp, review
        String[] parts = line.split("\t");
        if (parts.length < 4) return;  // Skip incomplete lines
        
        String mainCategory = parts[1];  // "Media", "Video Games", etc.
        String timestampStr = parts[2]; // Unix timestamp in milliseconds
        String review = parts[3];       // Review text
        
        // Extract year from the Unix timestamp (in milliseconds)
        long timestamp = Long.parseLong(timestampStr);
        Date date = new Date(timestamp);  // Convert timestamp to Date object
        
        // Get the year from the Date object
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);
        
        // Emit key-value pair: (mainCategory_year, review)
        context.write(new Text(mainCategory + "," + year), new Text(review));
    }
}