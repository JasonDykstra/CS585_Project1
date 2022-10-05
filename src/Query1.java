import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Query1 {
    public static class AgeMapper extends Mapper<Object, Text, IntWritable, Text>{
        //new customer
        private final static Text customer = new Text();
        //mapper to check for age range
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
    //no need for reduce
    public static void main(String[] args) throws Exception {
//        args = new String[3];
//        args[0] = "/Users/maxine/Files/cs585/CS585_Project1/data/customers.csv";
//        args[1] = "/Users/maxine/Files/cs585/CS585_Project1/data/transactions.csv";
//        args[2] = "/Users/maxine/Files/cs585/CS585_Project1/data/output1.txt";
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