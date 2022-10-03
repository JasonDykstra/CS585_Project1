import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1 {
    public static class AgeMapper extends Mapper<Object, Text, IntWritable, Text>{
        //new customer
        private final static Text customer = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String customerString = value.toString();
            String[] customerData = customerString.split(",");

            int customerAge = Integer.parseInt(customerData[2]);
            if (20 <= customerAge && customerAge <= 50) {
                customer.set(value);
                context.write(null, customer);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query1.class);
        job.setMapperClass(AgeMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}