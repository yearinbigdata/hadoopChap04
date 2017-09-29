package com.world.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf);
		//job 생성
		
		job.setJobName("World Word Count"); //잡의 이름 정하기
		
		
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		//Mapper, Reducer 설정
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//키와 값의 클래스 설정
		
		FileSystem hdfs = FileSystem.get(conf);
		
		Path inDir = new Path("/home/java/world/city.csv");
		Path outDir= new Path("/home/java/world_city");
		//아래 폴더 안에 결과가 쓰여짐
		
		if(hdfs.exists(outDir)){//결과 받을 폴더가 있을 경우 지워야 에러 안남
			hdfs.delete(outDir, true);
		}
		hdfs.close();
		
		FileInputFormat.setInputPaths(job, inDir);
		//맵리듀스프로그램의 입력. job을 만들었는데 이것의 입력으로는 이 파일을 지정하는 것
		//이 파일이 Mapper로 들어온다. 한 라인씩. 
		
		FileOutputFormat.setOutputPath(job, outDir);
		//출력도 hdfs에 저장. Reducer의 결과가 이 폴더가 만들어지면서 여기에 저장된다.
		
		job.waitForCompletion(true);
		//jobTracker에게 요청이 들어감. 

	}

}
