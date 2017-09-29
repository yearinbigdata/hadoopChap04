package com.world.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper에서 정해진 타입을 사용해야 한다
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// Mapper는 변환처리를 하는 것. 입력은 다양할 수 있는데, 무엇을 입력하느냐에 따라서 입력 타입이 결정된다.
	// 처리 후 결과를 출력하는 타입이 뒤의 두 가지이다. 출력타입은 개발자가 정한다.

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("########");
		System.out.println("setup...");
		System.out.println("########");
	}

	static int cnt;
	@Override // 한 라인씩 읽어서 그 정보를 입력함. key에는 라인넘버, value에는 한 라인 내용이 들어옴
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("cnt=" + cnt++ + " : " +  key.get() + ":" + value);

		//value 한 라인씩 -> 쪼갠다
		String[] values = value.toString().split(",");
		
		//reducer에게 넘겨주기 -> 출력타입으로 변환작업 해서 넘어간다. text: 키. IntWritable:단어의 개
		for (String v : values) {
			context.write(new Text(v), new IntWritable(1));
			//집계하기 편하게 하기 위한 준비. 단어가 있으면 한 번은 발생하기 때문에 1을 넣음
			System.out.println(v + "\t1");
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		System.out.println("##########");
		System.out.println("cleanup...");
		System.out.println("##########");
	}

}
