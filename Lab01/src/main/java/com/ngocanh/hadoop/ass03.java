package com.ngocanh.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ass03 extends Configured implements Tool {

    private static final String TAG_MOVIE = "M";
    private static final String TAG_RATING = "R";
    private static final String TAG_USER = "U";


    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 3);
            context.write(new Text(parts[0].trim()), new Text(TAG_MOVIE + "," + parts[1].trim()));
        }
    }


    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            String movieId = parts[1].trim();
            String userId = parts[0].trim();
            String rating = parts[2].trim();

            context.write(new Text(movieId), new Text(TAG_RATING + ',' + userId + "," + rating));
        }
    }


    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            String userId = parts[0].trim();
            String gender = parts[1].trim();

            context.write(new Text(userId), new Text(TAG_USER + "," + gender));
        }
    }


    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, String> userGenderMap = new HashMap<>();

        private String currentMovieTitle = "";
        private List<String[]> ratingBuffer = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith(TAG_USER)) {
                    // build user map
                    String gender = v.substring(2);
                    userGenderMap.put(key.toString(), gender);
                }
            }

            // reset
            currentMovieTitle = "";
            ratingBuffer.clear();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith(TAG_MOVIE)) {
                    currentMovieTitle = v.substring(2);
                } else if (v.startsWith(TAG_RATING)) {
                    String[] parts = v.split(",");
                    ratingBuffer.add(new String[]{parts[1], parts[2]}); // userId, rating
                }
            }

            if (!currentMovieTitle.isEmpty()) {
                double maleSum = 0, femaleSum = 0;
                int maleCount = 0, femaleCount = 0;

                for (String[] r : ratingBuffer) {
                    String userId = r[0];
                    double rating = Double.parseDouble(r[1]);

                    String gender = userGenderMap.get(userId);

                    if (gender == null) continue;

                    if (gender.equals("M")) {
                        maleSum += rating;
                        maleCount++;
                    } else if (gender.equals("F")) {
                        femaleSum += rating;
                        femaleCount++;
                    }
                }

                double maleAvg = maleCount > 0 ? maleSum / maleCount : 0;
                double femaleAvg = femaleCount > 0 ? femaleSum / femaleCount : 0;

                context.write(new Text(currentMovieTitle),
                        new Text(String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg)));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Lab01: Assign03 - Rating by Gender");
        job.setJarByClass(ass03.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);

        for (int i = 1; i < args.length - 2; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, RatingMapper.class);
        }

        // users.txt
        MultipleInputs.addInputPath(job, new Path(args[args.length - 2]), TextInputFormat.class, UserMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ass03(), args);
        System.exit(res);
    }
}

