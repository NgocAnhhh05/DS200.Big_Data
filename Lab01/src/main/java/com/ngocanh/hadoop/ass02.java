package com.ngocanh.hadoop;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ass02 extends Configured implements Tool{
    private static final String TAG_GENRE = "G";
    private static final String TAG_RATING = "R";

    public static class GenresMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] parts = value.toString().split(",", 3);
            outKey.set(parts[0].trim());
            String genres = parts[2].trim();
            outValue.set(TAG_GENRE + "," + genres);
            context.write(outKey, outValue);
            }
        }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] parts = value.toString().split(",", 3);
            outKey.set(parts[1].trim());
            outValue.set(TAG_RATING + "," + parts[2].trim());
            context.write(outKey, outValue);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();

        private TreeMap<String, double[]> genreValues = new TreeMap<>();

        @Override
        public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException{
            String genres = "";
            double sumRating = 0.0;
            int count = 0;

            for (Text val: values){
                String strVal = val.toString();
                if (strVal.startsWith(TAG_GENRE)){
                    genres = strVal.substring(2);
                }
                else if (strVal.startsWith(TAG_RATING)){
                    try{
                        sumRating += Double.parseDouble(strVal.substring(2));
                        count++;
                    }
                    catch(NumberFormatException e){}
                }
            }
            if (count > 0 && !genres.isEmpty()){
                String[] listOfGenres = genres.split("\\|");
                for (String genre : listOfGenres){
                    genre = genre.trim();
                    genreValues.putIfAbsent(genre, new double[]{0.0, 0.0});

                    double[] genreValue = genreValues.get(genre);
                    genreValue[0] += sumRating;
                    genreValue[1] += count;

                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            for (Map.Entry<String, double[]> entry : genreValues.entrySet()){
                String genre = entry.getKey();
                double sum = entry.getValue()[0];
                int count = (int) entry.getValue()[1];
                double avgRating = sum /count;

                outKey.set(genre);
                outValue.set(String.format("Average Rating: %.2f, Count: %d", avgRating, count));
                context.write(outKey, outValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Lab01 - Assign 02: Calculate rating by genres");
        job.setJarByClass(ass02.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GenresMapper.class);

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
        int exitCode = ToolRunner.run(new Configuration(), new ass02(), args);
        System.exit(exitCode);
    }

}
