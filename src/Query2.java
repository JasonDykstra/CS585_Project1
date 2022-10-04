import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
        private final static Text outValue = new Text();
        private final static Text outID = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String customerString = value.toString();
            String[] customerData = customerString.split(",");
            String customerID = customerData[0];
            String customerName = customerData[1];
            outID.set(customerID);
            outValue.set(String.join(",", "customer", customerName));
            context.write(outID, outValue);
        }
    }

    public static class TransactionsMapper extends Mapper<Object, Text, Text, Text> {

        private final static Text outID = new Text();
        private final static Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String transactionString = value.toString();
            String[] transactionData = transactionString.split(",");
            String customerID = transactionData[1];
            String transactionAmount = transactionData[2];
            outID.set(customerID);
            outValue.set(String.join(",", "transaction", transactionAmount));
            context.write(outID, outValue);
        }
    }

    public static class SumTransactionReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text output = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String customerName = "";
            int numTransactions = 0;
            int totalSum = 0;

//            String testOutput = "";
//            Iterator<Text> iter = value.iterator();
//            while(iter.hasNext()){
//                testOutput += (" " + iter.next());
//            }

            for(Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("transaction")){
                    numTransactions += 1;
                    totalSum += Integer.parseInt(str[1]);
                } else if(str[0].equals("customer")){
                    customerName = str[1];
                }
            }

//            System.out.println(customerName + "," + numTransactions + "," + totalSum);

            output.set(String.join(",", customerName, Integer.toString(numTransactions), Integer.toString(totalSum)));


            context.write(key, output);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query2");
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