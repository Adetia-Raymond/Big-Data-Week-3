import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SpecificElementReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the occurrences of the element
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Write the result (element and its count)
        context.write(key, new IntWritable(sum));
    }
}
