package HadoopFileSystems;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import java.net.URI;

public class FileSystemDoubleCat 
{

	/**
	 * @param args[0] - hdfs path to source file.
	 */
	public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try
		{
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0);  // Go back to the start of the file
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		finally
		{
			IOUtils.closeStream(in);
		}
	}

}
