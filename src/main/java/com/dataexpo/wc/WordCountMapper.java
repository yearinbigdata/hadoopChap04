package com.dataexpo.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String[] values = value.toString().split(",");//쪼개기
		//필요한 것
		String year = values[0];
		String month = values[1];
		output.collect(new Text(year + month), new IntWritable(1));
		//연도와 월을 키로 해서, 있었으면 1이 나옴
		//한 달에 여러 건이 있기 때문에 중복되어 여러개 나옴
		
		//리듀스에 넘겨주기 전에 프레임워크가 sort한다. 
		//년월은 키로, 1들은 list로 넘겨서 Reducer에서 sum함으로서 횟수가 나온다
		
	}

}
