import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {


    public static class NormalizeMapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //inputvalue : movieA:movieB \t sum
            //output key : movieA
            //output value : movieB:sum
            String[] movies_relation = value.toString().trim().split("\t");
            String movieA = movies_relation[0].split(":")[0];
            String movieB = movies_relation[0].split(":")[1];
            String relation = movies_relation[1];
            context.write(new Text(movieA), new Text(movieB + ":" + relation));
        }
    }


    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //input key : movieA
            //input value : movieB:relation
            //output key : movieB
            //output value : movieA:relation/sumOfrelations
            int total = 0 ;
            Map<String,Integer> map = new HashMap<String,Integer>();
            while(values.iterator().hasNext()) {
                String[] movie_relation = values.iterator().next().toString().split(":");
                String movieB = movie_relation[0];
                int relation = Integer.parseInt
                        (movie_relation[1]);
                total += relation;
                map.put(movieB,relation);
            }

            for(Map.Entry<String,Integer> entry : map.entrySet()) {
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double) entry.getValue() / total;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Normalize.class);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }


}
