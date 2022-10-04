import java.io.IOException;
import java.util.*;

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
import org.apache.hadoop.util.hash.Hash;

public class Query4 {
    /*
    Country Code, Num Customers, Min trans total, max trans total
    610,          100,           4500,            9900

    Min and max trans total are the transaction totals of all customers in a country code

    Possibly have each mapper output to key of customer ID, sum up everything in reducer and filter after all data has been processed.
     */

    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        // Should return <customerID, "customer,countryCode">

        private final static Text outValue = new Text();
        private final static Text outID = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String customerString = value.toString();
            String[] customerData = customerString.split(",");
            String customerID = customerData[0];
            String customerCountryCode = customerData[4];
            outID.set(customerID);
            outValue.set(String.join(",", "customer", customerCountryCode));
            context.write(outID, outValue);
        }
    }

    public static class TransactionsMapper extends Mapper<Object, Text, Text, Text> {
        // Should return <customerID, "transaction,transactionAmount">

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
        private final static Text outKey = new Text();
        private final static Text outValue = new Text();

        // Hash maps to keep track of per-area-code data
        private static HashMap<Integer, Integer> areaCodeCounts = new HashMap<>();
        private static HashMap<Integer, Double> areaCodeMin = new HashMap<>();
        private static HashMap<Integer, Double> areaCodeMax = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Store area code of customers
            int areaCode = 0;

            // Store transactions in array
            ArrayList<Double> transactions = new ArrayList<>();


            for(Text val : values) {
                String[] str = val.toString().split(",");
                if(str[0].equals("transaction")){
                    // Transaction
                    double transactionAmount = Double.parseDouble(str[1]);
                    transactions.add(transactionAmount);

                    // Update smallest transactions
                    if(areaCodeMin.containsKey(areaCode)){
                        areaCodeMin.put(areaCode, Math.min(areaCodeMin.get(areaCode), transactionAmount));
                    } else {
                        areaCodeMin.put(areaCode, transactionAmount);
                    }

                    // Update largest transactions
                    if(areaCodeMax.containsKey(areaCode)){
                        areaCodeMax.put(areaCode, Math.max(areaCodeMax.get(areaCode), transactionAmount));
                    } else {
                        areaCodeMax.put(areaCode, transactionAmount);
                    }
                } else if(str[0].equals("customer")) {
                    // Customer
                    int customerAreaCode = Integer.parseInt(str[1]);
                    areaCode = customerAreaCode;

                    // Update area code hash map
                    if(areaCodeCounts.containsKey(areaCode)){
                        // If key exists, add 1 to it
                        areaCodeCounts.put(areaCode, areaCodeCounts.get(areaCode) + 1);
                    } else {
                        // Otherwise, create a new key
                        areaCodeCounts.put(areaCode, 1);
                    }
                }
            }

            // Loop over hash maps and write the area code as key and customers-, min transaction-, and max transaction-per-area-code as value
            for(Map.Entry<Integer, Integer> set : areaCodeCounts.entrySet()){
                int hkey = set.getKey();
                int hval = set.getValue();

                double min = areaCodeMin.get(hkey);
                double max = areaCodeMax.get(hkey);

                String outString = String.join(",", Integer.toString(hval), Double.toString(min), Double.toString(max));
                outKey.set(Integer.toString(hkey));
                outValue.set(outString);
                context.write(outKey, outValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query4");
        job.setJarByClass(Query4.class);
//        job.setMapperClass(Query4.CustomerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        job.setCombinerClass(Query4.SumTransactionReducer.class);
        job.setReducerClass(Query4.SumTransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query4.CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query4.TransactionsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
