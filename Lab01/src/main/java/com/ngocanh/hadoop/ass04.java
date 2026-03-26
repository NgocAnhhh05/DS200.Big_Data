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

public class ass04 extends Configured implements Tool {

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

            context.write(new Text(movieId), new Text(TAG_RATING + "," + userId + "," + rating));
        }
    }

    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            String userId = parts[0].trim();
            String age = parts[2].trim();

            context.write(new Text(userId), new Text(TAG_USER + "," + age));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Integer> userAgeMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String movieTitle = "";
            List<String[]> ratings = new ArrayList<>();

            // Copy values ra list vì Hadoop iterator chỉ dùng 1 lần
            List<String> temp = new ArrayList<>();
            for (Text val : values) {
                temp.add(val.toString());
            }

            // 1. Build user map
            for (String v : temp) {
                if (v.startsWith(TAG_USER)) {
                    int age = Integer.parseInt(v.substring(2));
                    userAgeMap.put(key.toString(), age);
                }
            }

            // 2. Lấy movie + rating
            for (String v : temp) {
                if (v.startsWith(TAG_MOVIE)) {
                    movieTitle = v.substring(2);
                } else if (v.startsWith(TAG_RATING)) {
                    String[] parts = v.split(",");
                    ratings.add(new String[]{parts[1], parts[2]}); // userId, rating
                }
            }

            if (!movieTitle.isEmpty()) {

                double[] sum = new double[4];
                int[] count = new int[4];

                for (String[] r : ratings) {
                    String userId = r[0];
                    double rating = Double.parseDouble(r[1]);

                    Integer age = userAgeMap.get(userId);
                    if (age == null) continue;

                    int group = getAgeGroup(age);

                    sum[group] += rating;
                    count[group]++;
                }

                String result = String.format(
                    "0-18: %s   18-35: %s   35-50: %s   50+: %s",
                    avg(sum[0], count[0]),
                    avg(sum[1], count[1]),
                    avg(sum[2], count[2]),
                    avg(sum[3], count[3])
                );

                context.write(new Text(movieTitle), new Text(result));
            }
        }

        private int getAgeGroup(int age) {
            if (age <= 18) return 0;
            else if (age <= 35) return 1;
            else if (age <= 50) return 2;
            else return 3;
        }

        private String avg(double sum, int count) {
            if (count == 0) return "NA";
            return String.format("%.2f", sum / count);
        }

    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Lab01: Assign04 - Rating by Age Group");
        job.setJarByClass(ass04.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);

        for (int i = 1; i < args.length - 2; i++) {
            MultipleInputs.addInputPath(job, new Path(args[i]), TextInputFormat.class, RatingMapper.class);
        }

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
        int res = ToolRunner.run(new Configuration(), new ass04(), args);
        System.exit(res);
    }
}