package org.hadoop.cn;

import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCount extends Configured implements Tool{

	//1.Map class
	//参数：偏移量+内容->内容+计数
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text mapOutputKey = new Text();
		private IntWritable mapOutputValue = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//将读取的文件变成：偏移量+内容
			String linevalue = value.toString();
			//System.out.println("linevalue----"+linevalue);
			//根据" "去切分我们的单词,并处理
			String[] strs=linevalue.split(" ");
			for (String str : strs) {
				//key:单词 value：1
				mapOutputKey.set(str);
				mapOutputValue.set(1);	
				context.write(mapOutputKey, mapOutputValue);
				//System.out.println("<"+mapOutputKey+","+mapOutputValue+">");
			}
			//将结果传递出去
		}
	}
	
	//2.Reducer class
	//参数：内容+计数->内容+计数
	public static class WordCountRecucer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			//汇总
			int sum = 0;
			for(IntWritable value:values) {
				sum+=value.get();
				//System.out.println(value.get()+" ");
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}
	
	//3.job class
	public int run(String[] args)throws Exception{
		//获取我们的配置
		Configuration conf =new Configuration();
		conf.set("fs.defaultFS","hdfs://oym2.com:8082");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapreduce.job.jar","D:\\hadoopProject\\mapreduce\\wordcount\\out\\artifacts\\wordcount_jar\\wordcount.jar");
		Job job=Job.getInstance(conf, this.getClass().getSimpleName());

		job.setJar("D:\\hadoopProject\\mapreduce\\wordcount\\out\\artifacts\\wordcount_jar\\wordcount.jar");

		//设置input与output
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		
		
		//设置map与reduce
		//需要设置的内容 类+输出key与value
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(WordCountRecucer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean issuccess = job.waitForCompletion(true);
		
		
		return issuccess?0:1;
		}

	public static void main(String[] args) throws Exception {
		//参数
	/*	args = new String[] {
				"hdfs://oym2.com:8020/user/root/mapreduce/input",
				"hdfs://oym2.com:8020/user/root/mapreduce/output"
		};*/

		/*args = new String[] {
				"hdfs://oym2.com:8020/word_input",
				"hdfs://oym2.com:8020/outpu"
		};	*/

		args = new String[] {
				"/word_input/words.txt",
				"/output4"
		};
		//跑我们的任务
		int status =new WordCount().run(args);
		System.exit(status);
	}
	
}
