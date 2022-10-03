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

//referenced https://www.edureka.co/blog/mapreduce-example-reduce-side-join/
public class Query3 {

    // mapper for Customer
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String ID = str[0];
            String name = str[1];
            String salary = str[5];
            outKey.set(ID);
            outValue.set(String.join(",", "customer", name, salary));
            context.write(outKey, outValue);
        }
    }

    // mapper for Transactions
    public static class TransactionMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            String ID = str[1];
            String count = "1";
            String totalSum = str[2];
            String numItems = str[3];
            outKey.set(ID);
            outValue.set(String.join(",", "transaction", count, totalSum, numItems));
            context.write(outKey, outValue);
        }
    }
    //combine inputs from both mapper's key value pair
    public static class Combiner extends Reducer<Text, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numTransactions = 0;
            float totalSum = 0;
            int minItem = 10;
            String name = "";
            String salary = "";
            boolean flag = false;
            for (Text val : values) {
                String[] str = val.toString().split(",");
                if (str[0].equals("transaction")){
                    numTransactions += Integer.parseInt(str[1]);
                    totalSum += Double.parseDouble(str[2]);
                    minItem = Math.min(minItem,Integer.parseInt(str[3]));
                    flag = true;
                }else if (str[0].equals("customer")){
                    name = str[1];
                    salary = str[2];
                }
            }
            if(flag){
                outKey.set(key);
                outValue.set(String.join(",", "transaction", String.valueOf(numTransactions), String.valueOf(totalSum), String.valueOf(minItem)));
            }else{
                outKey.set(key);
                outValue.set(String.join(",", "customer", name, salary));
            }
            context.write(outKey, outValue);
        }
    }

    // reduces both mapper and combiner input into final string form
    public static class FinalReducer extends Reducer<Text, Text, Text, NullWritable> {

        private Text outKey = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float totalSum = 0;
            int numTransactions = 0;
            int minItem = 10;
            String name = "";
            String salary = "";
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
                    name = str[1];
                    salary = str[2];
                }
            }
            outKey.set(String.join(",", key.toString(), name, salary, String.valueOf(numTransactions), String.valueOf(totalSum), String.valueOf(minItem)));
            context.write(outKey, NullWritable.get());
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