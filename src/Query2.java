import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query2 {
    /*
    Want:
    CustomerID, CustomerName, NumTransactions, TotalSum
     */
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        //new customer
        private final static Text customerID = new Text();
        private final static Text customerName = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String customerString = value.toString();
            String[] customerData = customerString.split(",");
            customerID.set(customerData[0]);
            customerName.set(customerData[1]);
            // Output <CustomerID, CustomerName> as key value pair
            context.write(customerID, customerName);
        }
    }

    public static class TransactionsMapper extends Mapper<Object, Text, Text, Text> {
//        private final static IntWritable numTransactions = new IntWritable();
//        private final static IntWritable totalSum = new IntWritable();
        private final static Text transactionAmount = new Text();
        private final static Text customerID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String transactionString = value.toString();
            String[] transactionData = transactionString.split(",");
            customerID.set(transactionData[1]);
            // Set the output to have one column for the transaction total, and a 1 for summing the number of transactions later
            transactionAmount.set(transactionData[2]);

            context.write(customerID, transactionAmount);
        }
    }

    public static class SumTransactionReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text output = new Text();

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            String customerName = "";
            int numTransactions = 0;
            int totalSum = 0;

            // Loop over the array of values, should look like: [customerName, transaction total, 1]
            for(Text str : value) {
                String[] valueData = value.toString().split(",");
                customerName = valueData[0];
                totalSum += Integer.parseInt(valueData[1]);
                numTransactions += 1;
            }

            output.set(customerName + "," + numTransactions + "," + totalSum);

            context.write(key, output);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query1");
        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2.CustomerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(Query2.SumTransactionReducer.class);
        job.setReducerClass(Query2.SumTransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}