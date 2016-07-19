package HadoopFileSystems;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import java.io.InputStream;
import java.net.URL;

public class URLCat {

	static 
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	/**
	 * @param args[0] - hdfs path to source file.
	 */
	public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		InputStream in = null;
		try
		{
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} 
		finally
		{
			IOUtils.closeStream(in);
		}

	}

}
