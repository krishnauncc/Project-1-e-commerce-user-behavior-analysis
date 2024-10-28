package com.ecommerce.tasks.Task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class PurchasingBehaviorReducer extends Reducer<Text, IntWritable, Text, Text> {

    // Store maximum count and corresponding peak hours for each category
    private final Map<String, Integer> maxCounts = new HashMap<>();
    private final Map<String, List<String>> peakHours = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Parse hour and category from the key
        String[] parts = key.toString().split(",");
        String hour = parts[0];
        String category = parts[1];

        // Sum up the counts for the current hour-category combination
        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }

        // Check if this is the maximum count observed for the category
        if (!maxCounts.containsKey(category) || count > maxCounts.get(category)) {
            maxCounts.put(category, count);
            peakHours.put(category, new ArrayList<>());
            peakHours.get(category).add(hour);
        } else if (count == maxCounts.get(category)) {
            peakHours.get(category).add(hour); // If count matches the max, add to peak hours
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output peak hours for each category based on max count
        for (Map.Entry<String, List<String>> entry : peakHours.entrySet()) {
            String category = entry.getKey();
            List<String> hours = entry.getValue();
            int maxCount = maxCounts.get(category);
            
            // Format output as "Category: Peak Hour(s) [Hour(s)] with Count"
            context.write(new Text(category), new Text("Peak Hour(s): " + String.join(", ", hours) + " with Count: " + maxCount));
        }
    }
}
