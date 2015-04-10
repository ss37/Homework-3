package edu.pitt.sis.infsci2711.lettercount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class counts the number of words that start with an alphabet 
 * and displays the output in a text file.
 * @author cloudera
 */
public class LetterCount extends Configured implements Tool {

	/**
	 * This method creates a job to perform the task.
	 */
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "LetterCount");
		job.setJarByClass(LetterCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	/**
	 * This method counts the number of words that starts with a particular alphabet.
	 * It ignores words that start with a special character.
	 * It ignores the letter case.
	 * @author cloudera
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//Split the string into words
			for (String token: value.toString().split("\\s+")) {
				//check if the token is empty and has a valid first character
				//if yes then set the word and keep track of it
				if(token!=null && !token.isEmpty() && token.substring(0,1) != null){
					Character c = token.charAt(0);
					if(Character.isLetter(c)){
						String t = c.toString().toLowerCase();
						word.set(t);
					}
				}
				context.write(word, ONE);
			}
		}
		
	}
	
	/**
	 * This method reduces the output from different nodes of a cluster
	 * Based on the first character of each word
	 * It finds the sum of all output for that character
	 * @author cloudera
	 *
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val: values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/** This is the start point of execution of the program.
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new LetterCount(), args);
		System.exit(res);
	}

}
