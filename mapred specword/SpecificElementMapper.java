import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class SpecificElementMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text elementKey = new Text();
    private String elementToCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        elementToCount = conf.get("elementToCount");
        elementKey.set(elementToCount);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line by commas (CSV format)
        String[] elements = value.toString().split(",");

        // Process each element in the CSV line
        for (String element : elements) {
            System.out.println("Processing element: " + element.trim());  
            if (element.trim().equalsIgnoreCase(elementToCount)) {
                System.out.println("Matched element: " + element.trim());  
                context.write(elementKey, one);
            }
        }
    }
}
