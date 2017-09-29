package com.world.wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//Mapper의 출력 키 타입(뒤 두개)이 Reducer의 입력키 타입(앞 두개)과 동일. 
//맵퍼의 변환처리 결과가 리듀서에 들어가기 때문이다.
//Mapper -> Reducer
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//key라는 그룹에 대해서 한번 들어옴. wc에서는 key에 하나의 단어가 들어오고, value는 발생 횟수이다.
		//발생횟수를 다 더함으로써 단어 개수만큼 reduce가 호출됨. 
		//Map 메소드는 라인 당 한번씩 호출됨. 
		
		List<Integer> list = new ArrayList<>();
		
		int sum=0;
		for (IntWritable v : values){
			list.add(v.get());
			sum += v.get();
			
		}
		
		context.write(key, new IntWritable(sum));
		
		System.out.println("key=" + key + ", value =" + list);
		
	}
}
