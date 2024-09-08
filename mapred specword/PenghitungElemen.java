import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PenghitungElemen {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: PenghitungElemen <input_path> <output_path> <element_to_count>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("elementToCount", args[2]);  

        // Set up the job
        Job job = Job.getInstance(conf, "specific element count");
        job.setJarByClass(PenghitungElemen.class);
        job.setMapperClass(SpecificElementMapper.class);
        job.setReducerClass(SpecificElementReducer.class);

        // Set the output types for Mapper and Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
