import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

    public static class SumTransactionCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Important: All key-value pairs with the same key will end up in the same reducer class, so we can pass
            // info from multiple mappers with different output information to the same reducer to aggregate the data.
            String customerName = "";
            double totalSum = 0.0;
            int numTransactions = 0;
            int isCustomer = 0;
            // Loop over the values passed into the reducer
            // The value will either be "customer,custName" or "transaction,transactionAmount"
            // values starting with customer must be handled differently than transactions
            for(Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("transaction")){
                    totalSum += Double.parseDouble(str[1]);
                    numTransactions += 1;
                } else if(str[0].equals("customer")) {
                    customerName = str[1];
                    isCustomer = 1;
                }
            }
            String outputStr = "";
            if (isCustomer == 1)
            {
                outputStr = String.join(",", "customer", customerName);
            }
            else
            {
                outputStr = String.join(",", "transaction", String.valueOf(numTransactions), String.valueOf(totalSum));
            }

            outValue.set(outputStr);
            context.write(key, outValue);
        }
    }

    public static class SumTransactionReducer extends Reducer<Text, Text, Text, NullWritable> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Important: All key-value pairs with the same key will end up in the same reducer class, so we can pass
            // info from multiple mappers with different output information to the same reducer to aggregate the data.
            String customerName = "";
            double totalSum = 0.0;
            int numTransactions = 0;
            int isCustomer = 0; //customer flag
            // Loop over the values passed into the reducer
            // The value will either be "customer,custName" or "transaction,transactionAmount"
            // values starting with customer must be handled differently than transactions
            for(Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("transaction")){
                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                } else if(str[0].equals("customer")) {
                    customerName = str[1];
                    isCustomer = 1;
                }
            }
            String outputStr = "";
            if (isCustomer == 1)
            {
                outputStr = String.join(",", key.toString(), customerName, String.valueOf(numTransactions), String.valueOf(totalSum));
            }
            else
            {
                // no customer, should not output
                outputStr = "";
            }

            outKey.set(outputStr);

            context.write(outKey, NullWritable.get());
        }
    }
    public static void main(String[] args) throws Exception {
        args = new String[3];
        args[0] = "data/customers.csv";
        args[1] = "data/transactions.csv";
        args[2] = "data/query2output";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query2");
        job.setJarByClass(Query2.class);
//        job.setMapperClass(Query2.CustomerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(Query2.SumTransactionCombiner.class);
        job.setReducerClass(Query2.SumTransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}