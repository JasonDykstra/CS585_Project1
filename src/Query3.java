import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
public class Query3 {

    // Mapper for Customer
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        private Text output_key = new Text();
        private Text output_value = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String ID = str[0];
            String name = str[1];
            String salary = str[5];
            output_key.set(ID);
            output_value.set(String.join(",", "customer", name, salary));
            context.write(output_key, output_value);
        }
    }

    // Mapper for Transactions
    public static class TransactionMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String ID = str[1];
            String count = "1";
            String totalSum = str[2];
            String numItems = str[3];
            outputKey.set(ID);
            outputValue.set(String.join(",", "transaction", count, totalSum, numItems));
            context.write(outputKey, outputValue);
        }
    }
    //combine inputs from both mapper's key value pair
    public static class Combiner extends Reducer<Text, Text, Text, Text>{
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numTransactions = 0;
            float totalSum = 0;
            int minItem = 10;
            String customerName = "";
            String customerSalary = "";
            boolean flag = false;
            for (Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("transaction")){
                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                    minItem = Math.min(minItem,Integer.parseInt(str[3]));
                    flag = true;
                }else if (str[0].equals("customer")){
                    customerName = str[1];
                    customerSalary = str[2];
                }
            }
            if(flag){
                outputKey.set(key);
                outputValue.set(String.join(",", "transaction", String.valueOf(numTransactions), String.valueOf(totalSum), String.valueOf(minItem)));
            }else{
                outputKey.set(key);
                outputValue.set(String.join(",", "customer", customerName, customerSalary));
            }
            context.write(outputKey, outputValue);
        }
    }

    // reduces both mapper and combiner input into final string form
    public static class FinalReducer extends Reducer<Text, Text, Text, NullWritable> {

        private Text outputKey = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float totalSum = 0;
            int numTransactions = 0;
            int minItem = 10;
            String customerName = "";
            String customerSalary = "";
            for (Text val : values) {
                String[] str = val.toString().split(",");
                //checks if from transaction
                if (str[0].equals("transaction")) {
                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                    minItem = Math.min(minItem,Integer.parseInt(str[3]));
                }
                //checks if from customer
                else if (str[0].equals("customer")){
                    customerName = str[1];
                    customerSalary = str[2];
                }
            }
            outputKey.set(String.join(",", key.toString(), customerName, customerSalary, String.valueOf(numTransactions), String.valueOf(totalSum), String.valueOf(minItem)));
            context.write(outputKey, NullWritable.get());
        }
    }

    // Main Function
    public static void main(String[] args) throws Exception {
//        args = new String[3];
//        args[0] = "/Users/maxine/Files/cs585/Project1/input/customers.csv";
//        args[1] = "/Users/maxine/Files/cs585/Project1/input/transactions.csv";
//        args[2] = "/Users/maxine/Files/cs585/Project1/output/output2.txt";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Query3");

        job.setJarByClass(Query3.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(FinalReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,CustomerMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}