package mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;


public class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
	static enum myCounters {NUM_RECORDS};
	
	private static final int MISSING = 9999;
	private String mapTaskId;
    private String inputFile;
    private int noRecords = 0;
	
	public void configure(JobConf job)
	{
		mapTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
        inputFile = job.get(JobContext.MAP_INPUT_FILE);
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
	{
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		
		if (line.charAt(87) == '+')  //parseInt doesn't like leading + signs
		{
			airTemperature = Integer.parseInt(line.substring(88, 92));
		}
		else
		{
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		reporter.progress();
		String quality = line.substring(92, 93);
		
		noRecords ++;
		reporter.incrCounter(myCounters.NUM_RECORDS, 1);
		
		if (noRecords % 100 == 0)
		{
			reporter.setStatus(mapTaskId + " Processed " + noRecords + " from input file " + inputFile);
		}
		
		if (airTemperature != MISSING && quality.matches("[01459]"))
		{
			output.collect(new Text(year), new IntWritable(airTemperature));
		}
	}

}
