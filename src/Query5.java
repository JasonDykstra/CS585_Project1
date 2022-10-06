import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

//referenced https://www.edureka.co/blog/mapreduce-example-reduce-side-join/
public class Query5 {
    // mapper for Transaction
    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {
        //ageRange identifier
        private int ageRange(int age){
            if (age >= 10 && age < 20)
                return 0;
            else if (age >= 20 && age < 30)
                return 1;
            else if (age >= 30 && age < 40)
                return 2;
            else if (age >= 40 && age < 50)
                return 3;
            else if (age >= 50 && age < 60)
                return 4;
            else if (age >= 60 && age <= 70)
                return 5;
            else return 6;
        }
        //customer data as a hashmap
        private HashMap<Integer, String[]> customer = new HashMap<>();

        //setup processes customerData
        //Distrubuted cache utilization https://buhrmann.github.io/hadoop-distributed-cache.html
        public void setup(Context context) throws IOException, InterruptedException{
            URI[] cFiles = context.getCacheFiles();
            if (cFiles != null && cFiles.length > 0)
            {
                Path path = new Path(cFiles[0].toString());
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line = null;
                while((line = reader.readLine()) != null) {
                    String[] str = line.split(",");
                    Integer ID = Integer.parseInt(str[0]);
                    Integer age = ageRange(Integer.parseInt(str[2]));
                    String gender = str[3];
                    String[] ls = {Integer.toString(age), gender};
                    customer.put(ID,ls);
                }
            }
        }
        private Text outKey = new Text();
        private Text outVal = new Text();

        // mapper for Customer
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] custom = value.toString().split(",");
            String[] ls = customer.get(Integer.parseInt(custom[1]));
            outKey.set(ls[0] + ',' + ls[1]);
            outVal.set(custom[2]);
            context.write(outKey, outVal);
        }
    }


    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {
        //age hashmap
        private HashMap<Integer, String> ageRange = new HashMap<Integer, String>() {{
            put(0, "[10, 20)");
            put(1, "[20, 30)");
            put(2, "[30, 40)");
            put(3, "[40, 50)");
            put(4, "[50, 60)");
            put(5, "[60, 70]");
            put(6, "Outside Age Range");
        }};

        private Text outKey = new Text();

        // Reducer for transaction and age range
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double min = 5555.55;
            double max = 0.0;
            double sum = 0;
            int x = 0;
            for (Text t : values)
            {
                double trans = Double.parseDouble(t.toString());

                if(min > trans) min = trans;
                if(max < trans) max = trans;
                sum += trans;
                x++;
            }
            double avg = sum/x;
            String[] keyV = key.toString().split(",");
            outKey.set(ageRange.get(Integer.parseInt(keyV[0]))); //age range as outkey
            context.write(outKey, new Text(keyV[1] + "," + min + "," + max + "," + avg ));
        }
    }

    // Main Function
    public static void main(String[] args) throws Exception {
//        args = new String[3];
//        args[0] = "/Users/maxine/Files/cs585/CS585_Project1/data/customers.csv";
//        args[1] = "/Users/maxine/Files/cs585/CS585_Project1/data/transactions.csv";
//        args[2] = "/Users/maxine/Files/cs585/CS585_Project1/data/output5.txt";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//      job.setCombinerClass();
        job.addCacheFile(new URI(args[0]));
        job.setJarByClass(Query5.class);
        job.setMapperClass(TransactionMapper.class);
        job.setReducerClass(AgeReducer.class);
//      job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}