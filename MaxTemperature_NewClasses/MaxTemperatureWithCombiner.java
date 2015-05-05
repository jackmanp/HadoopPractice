package mapReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class MaxTemperatureWithCombiner 
{
	public static void main(String[] args) throws Exception
	{
		if (args.length != 2)
		{
			System.err.println("Usage: MaxTemperatureWithCombiner <input path> <output path>");
			System.exit(-1);
		}
		
		JobConf jobConf = new JobConf(new Configuration(), MaxTemperatureWithCombiner.class);
		jobConf.setJobName("Max Temperature");
		
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		jobConf.setMapperClass(MaxTemperatureMapper.class);
		jobConf.setCombinerClass(MaxTemperatureReducer.class);
		jobConf.setReducerClass(MaxTemperatureReducer.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(jobConf);
	}
}
