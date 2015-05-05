package mapReduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;

public class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
	private int noKeys = 0;
	private String reduceTaskID;
	
	public void configure(JobConf job)
	{
		reduceTaskID = job.get(JobContext.TASK_ATTEMPT_ID);
	}
	
	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
	{
		int noValues = 0;
		int maxValue = Integer.MIN_VALUE;
		
		while (values.hasNext())
		{
			maxValue = Math.min(maxValue, values.next().get());
			noValues++;
		}
		
		if (noValues % 10 == 0)
		{
			 reporter.progress();
		}
	    noKeys ++;  	
		output.collect(key,  new IntWritable(maxValue));
		
		if (noKeys % 100 == 0)
		{
			reporter.setStatus(reduceTaskID + " Processed " + noKeys);
		}
	}
}
