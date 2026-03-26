package com.ngocanh.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ass01 extends Configured implements Tool{
    private static final String TAG_MOVIE = "M";
    private static final String TAG_RATING = "R";

    // Mapper for movies.txt [MovieId: Title]
    public static class MovieMapper extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] parts = value.toString().split(",", 3);
            outKey.set(parts[0].trim());
            outValue.set(TAG_MOVIE + "," + parts[1].trim());
            context.write(outKey, outValue);
        }
    }

    // Mapper for ratings.txt [MovieId: Rating]
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] parts = value.toString().split(",");
            outKey.set(parts[1].trim());
            outValue.set(TAG_RATING + "," + parts[2].trim());
            context.write(outKey, outValue);
        }
    }
    // Reducer
    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        private String maxMovie = "";
        private double maxRating = 0.0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String movieTitle = "Unknown";
            double sumRating = 0.0;
            int count = 0;

            for (Text val : values){
                String strVal = val.toString();
                if (strVal.startsWith(TAG_MOVIE)){
                    movieTitle = strVal.substring(2);
                }
                else if (strVal.startsWith(TAG_RATING)){
                    try{
                        sumRating += Double.parseDouble(strVal.substring(2));
                        count++;
                    }
                    catch (NumberFormatException e) {}
                }
            }
            if (count > 0) {
                double avgRating = sumRating / count;
                outKey.set(movieTitle);
                outValue.set(String.format("AverageRating: %.2f (TotalRatings: %d)", avgRating, count));
                context.write(outKey, outValue);

                if (count >= 5){
                    if (avgRating > maxRating){
                        maxRating = avgRating;
                        maxMovie = movieTitle;
                    }
                }
            }
        }

        // cleanup()
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            if(!maxMovie.isEmpty()){
                String result = String.format("%s is is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings", maxMovie, maxRating);
                outKey.set(result);
                outValue.set("");
                context.write(outKey, outValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job  = Job.getInstance(getConf(), "Lab 01 - Assign 01: Calculate Movie Average Rating");
        job.setJarByClass(ass01.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        for (int i = 1; i < args.length - 1; i++){
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, RatingMapper.class);
        }
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new Configuration(), new ass01(), args);
        System.exit(exitCode);
    }
}

