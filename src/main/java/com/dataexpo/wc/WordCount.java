package com.dataexpo.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCount {

	public static void main(String[] args) throws IOException {
		
		JobConf job = new JobConf(); //Configuration 역할까지 한다
		
		job.setJobName("DataExpo Word Count");	//식별 위해 job 이름 정하기
		
		//3가지 클래스 로딩 필수
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//입출력 정하기 필수
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//입,출력 받는 것 생성
		Path inDir = new Path("/home/java/dataexpo/2008_nohead.csv");
		Path outDir = new Path("/home/java/dataexpo_2008_out");
		
		//폴더가 이미 있는 경우 지우기(에러나지 않도록)
		FileSystem hdfs = FileSystem.get(new Configuration());
		if (hdfs.exists(outDir)){
			hdfs.delete(outDir, true);
		}
		
		//입,출력 받는 것 설정
		FileInputFormat.setInputPaths(job, inDir);
		FileOutputFormat.setOutputPath(job, outDir);
		
		
		
		JobClient.runJob(job);	//job 수행 요청: jobClient가 받아서 어디의 jobtracker(big2,3,4중)에서 실행할지 결정한다.

	}

}
