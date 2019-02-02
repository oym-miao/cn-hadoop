package org.hadoop.cn.chartlink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
//2.在map执行之前，将文件放进内存
//3.reduce直接输出结果
public class MapSideJoin extends Configured implements Tool {

	private static final String CUSTOMER_LIST = "hdfs://lee01.cniao5.com:8020/user/root/mapreduce/user/file1";

	// 输出类型<key value>
	// key:城市编号 value：UserCity
	public static class MSJMapper extends Mapper<LongWritable, Text, CustOrderMapOutKey, Text> {

		private static final Map<Integer, CustomerBean> CUSTOMER_MAP = new HashMap<Integer, CustomerBean>();
		private final Text outputValue = new Text();
		private final CustOrderMapOutKey outputKey = new CustOrderMapOutKey();
		
		// map之前的准备工作，用来提高效率
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 小表读取，并传输
			FileSystem fs = FileSystem.get(URI.create(CUSTOMER_LIST), context.getConfiguration());
			FSDataInputStream fdis = fs.open(new Path(CUSTOMER_LIST));
			// 进行一下数据验证
			BufferedReader sr = new BufferedReader(new InputStreamReader(fdis));
			String line = null;
			String[] splits = null;
			while ((line = sr.readLine()) != null) {
				splits = line.split(" ");
				// 进行数据是否完整的验证，亦即是说，splits是否为4个
			}
			CustomerBean cb = new CustomerBean(Integer.parseInt(splits[0]), splits[1], splits[2], splits[3]);
			CUSTOMER_MAP.put(cb.getCustId(), cb);

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splits = line.split(" ");
			// 构建一下我们的数据
			int custID = Integer.parseInt(splits[1]);// 通过客户编号获取信息
			CustomerBean customerBean = CUSTOMER_MAP.get(custID);
			// 验证
			if (customerBean == null) { 
				return;
			}
			// 客户id + 名字 + 地址 + 电话 +消费
			StringBuffer sb = new StringBuffer();
			sb.append(splits[2]).append(" ").append(customerBean.getName()).append(" ").append(customerBean.getAddress()).append(" ").append(customerBean.getPhone());
			outputValue.set(sb.toString());
			outputKey.set(custID,Integer.parseInt(splits[0]));
			context.write(outputKey, outputValue);

		}
	}

	// reducer
	public static class RSJReducer extends Reducer<CustOrderMapOutKey, Text, CustOrderMapOutKey, Text> {

		@Override
		protected void reduce(CustOrderMapOutKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//原样输出
			for(Text value :values) {
				context.write(key, value);
			}
		}
	}

	// driver:任务相关设置
	public int run(String[] args) throws Exception {
		// 获取相关配置
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(MapSideJoin.class);
		// 设置input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		// 设置output
		Path outpath = new Path(args[1]);
		// 进行outpath的验真，存在即删除
		FileSystem fs = FileSystem.get(new URI(outpath.toString()), conf);
		if (fs.exists(new Path(outpath.toString()))) {
			fs.delete(new Path(outpath.toString()), true);
		}
		FileOutputFormat.setOutputPath(job, outpath);

		job.addCacheFile(URI.create(CUSTOMER_LIST));
		
		// 设置map
		job.setMapperClass(MSJMapper.class);
		job.setMapOutputKeyClass(CustOrderMapOutKey.class);
		job.setMapOutputValueClass(Text.class);

		// 设置reduce
		job.setReducerClass(RSJReducer.class);
		job.setOutputKeyClass(CustOrderMapOutKey.class);
		job.setOutputValueClass(Text.class);
		// 将job提交给Yarn
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// 保存的输入输出路径
		args = new String[] { "hdfs://lee01.cniao5.com:8020/user/root/mapreduce/city/file2",
				"hdfs://lee01.cniao5.com:8020/user/root/mapreduce/output" };
		// 将任务跑起来
		// int statas = new WordCountMapReduce().run(args);
		int statas = ToolRunner.run(conf, new MapSideJoin(), args);
		// 关闭我们的job
		System.exit(statas);
	}

}
