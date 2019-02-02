package org.hadoop.cn.chartlink;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.ToolRunner;

//1.构建一个新的bean
//2.map端根据表的长度来进行切分
//3.reduce根据flag来进行汇总
public class ReduceSideJoin extends Configured implements Tool{

	//输出类型<key value>
	//key:城市编号 value：UserCity
	public static class RSJMapper extends Mapper<LongWritable, Text, IntWritable, UserCity>{
				
		private UserCity user =null;
		private IntWritable outkey = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//切分数据 0->城市 1->user
			String line = value.toString();
			String[] splits=line.split(" ");
			//判断一下，数据来自于哪个文件，长度为2是city，长度为3是user
			if(splits.length==2) 
			{
				//city
				user = new UserCity();
				user.setCityID(splits[0]);
				user.setCityName(splits[1]);
				user.setFlag(0);
				outkey.set(Integer.parseInt(splits[0]));
				context.write(outkey, user);
			}
			else if(splits.length==3) {
				user = new UserCity();
				user.setUserID(splits[0]);
				user.setUserName(splits[1]);
				user.setCityID(splits[2]);
				user.setFlag(1);
				outkey.set(Integer.parseInt(splits[2]));
				context.write(outkey, user);
			}

		}
	}
	//reducer,map的输出就是我们reduce的输入
	public static class RSJReducer extends Reducer<IntWritable, UserCity, IntWritable, Text>{
		
		private List<UserCity> userCities = new ArrayList<UserCity>();
		private UserCity user =null;
		private Text outValue = new Text();
		
		@Override
		protected void reduce(IntWritable key, Iterable<UserCity> values,
				Context context) throws IOException, InterruptedException {
			
			//根据flag来进行区分
			userCities.clear();
			//values里面包含一个city 以及 多个user
			for(UserCity uc : values) {
				//唯一的一个city
				if(uc.getFlag()==0) {
					user = new UserCity(uc);
				}
				else if (uc.getFlag()==1) {
					//除了城市之外，其他用户的对象，将这些对象添加到集合
					userCities.add(new UserCity(uc));
				}				
			}
			
			//遍历集合，不玩城市信息
			for(UserCity u:userCities ) {
				//补全
				u.setCityName(user.getCityName());
				outValue.set(u.toString());
				//传递出去
				context.write(key, outValue);
			}
		}
	}
	
    
	//driver:任务相关设置
	public int run(String[] args) throws Exception{
		//获取相关配置
		Configuration conf = this.getConf();
		conf.set("fs.defaultFS","hdfs://oym2.com:8082");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapreduce.job.jar","D:\\hadoopProject\\mapreduce\\wordcount\\out\\artifacts\\wordcount_jar\\wordcount.jar");
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());

		job.setJar("D:\\hadoopProject\\mapreduce\\wordcount\\out\\artifacts\\wordcount_jar\\wordcount.jar");

		job.setJarByClass(ReduceSideJoin.class);
		//设置input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		//设置output
		Path outpath = new Path(args[1]);
		//进行outpath的验真，存在即删除
		FileSystem fs = FileSystem.get(new URI(outpath.toString()), conf);
		if(fs.exists(new Path(outpath.toString()))) {
			fs.delete(new Path(outpath.toString()));
		}		
		FileOutputFormat.setOutputPath(job, outpath);
		
		//设置map
		job.setMapperClass(RSJMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(UserCity.class);		
		
		//设置reduce
		job.setReducerClass(RSJReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//将job提交给Yarn
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//保存的输入输出路径
		args = new String[]{
			"/chart_input",
			"/char_out"
		};
		//将任务跑起来
		//int statas = new WordCountMapReduce().run(args);
		int statas = ToolRunner.run(conf, new ReduceSideJoin(), args);
		//关闭我们的job
		System.exit(statas);
	}

}
