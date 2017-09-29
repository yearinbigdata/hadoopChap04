package com.example.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCountJob {

	public static void main(String[] args) throws IOException {
		
		if (args.length != 2){
			System.out.println("Usage: WordCountJob input output");
			System.exit(-2);
		}

		JobConf job = new JobConf();
		
		job.setJobName("MyWordCount");
		
		job.setJarByClass(WordCountJob.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));//입력으로 뭐가 들어가는지
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//어디에 저장할 것인지
		
		JobClient.runJob(job);
		
	}

}
