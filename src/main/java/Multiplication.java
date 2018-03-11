import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {

    public static class MultiplicationRelationMapper extends Mapper<LongWritable,Text,Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().trim().split("\t");
            String outputKey = keyValue[0];
            String outputValue = keyValue[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class MultiplicationRatingMapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] user_movie_rating = value.toString().trim().split(",");
            String outputKey = user_movie_rating[1];
            String outputValue = user_movie_rating[0] + ":" + user_movie_rating[2];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text,Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //key movieB ignored
            //value1: movieA=relation
            //value2: user1:rating

            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String,Double>();

            for(Text value : values) {
                if(value.toString().trim().contains(":")) {
                    String[] user_rating = value.toString().trim().split(":");
                    ratingMap.put(user_rating[0],Double.parseDouble(user_rating[1]));
                } else {
                    String[] movie_relation = value.toString().trim().split("=");
                    relationMap.put(movie_relation[0],Double.parseDouble(movie_relation[1]));
                }
            }

            for(Map.Entry<String,Double> relationEntry : relationMap.entrySet()) {
                for(Map.Entry<String, Double> ratingEntry : ratingMap.entrySet()) {
                    context.write(new Text(ratingEntry.getKey() + ":" + relationEntry.getKey()),
                            new DoubleWritable(ratingEntry.getValue() * relationEntry.getValue()));
                }
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job,MultiplicationRelationMapper.class,LongWritable.class, Text.class,Text.class,Text.class,conf);
        ChainMapper.addMapper(job,MultiplicationRatingMapper.class,Text.class, Text.class,Text.class,DoubleWritable.class,conf);

        job.setMapperClass(MultiplicationRelationMapper.class);
        job.setMapperClass(MultiplicationRatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MultiplicationRelationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MultiplicationRatingMapper.class);


        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }


}
