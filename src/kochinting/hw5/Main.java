package kochinting.hw5;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) {
        // write your code here
        Job movieRatings = null;
        Configuration conf = new Configuration();

        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        conf.setStrings("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");

        //conf.setInt("mapred.reduce.tasks", 1);
        System.out.println ("======");
        try {
            movieRatings = new Job(conf, "MovieRatings");
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        // Specify the Input path
        try {
            FileInputFormat.addInputPath(movieRatings, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        // Set the Input Data Format
        movieRatings.setInputFormatClass(TextInputFormat.class);
        // Set the Mapper and Reducer Class
        movieRatings.setMapperClass(MovieRatingsMapper.class);
        movieRatings.setMapOutputKeyClass(CompositeKeyWritable.class);
        movieRatings.setMapOutputValueClass(Text.class);

        //movieRatings.setCombinerClass(MovieRatingsCombiner.class);
        movieRatings.setReducerClass(MovieRatingsReducer.class);
        movieRatings.setOutputKeyClass(Text.class);
        movieRatings.setOutputValueClass(Text.class);
        // Set the Jar file
        movieRatings.setJarByClass(Main.class);

        // Set the Output path
        FileOutputFormat.setOutputPath(movieRatings, new Path(args[1]));

        // Set the Output Data Format
        movieRatings.setOutputFormatClass(TextOutputFormat.class);

        // Set the Output Key and Value Class
        movieRatings.setOutputKeyClass(Text.class);
        movieRatings.setOutputValueClass(IntWritable.class);

        movieRatings.setNumReduceTasks(2);

        try {
            movieRatings.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
