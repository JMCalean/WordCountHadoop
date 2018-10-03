
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

@SuppressWarnings("deprecation")
public class distrFreqCount{
	public static class freqWordMapper
	extends Mapper<Object,Text,Text,IntWritable>{
				
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		
 		List<String> searchDictionary = new ArrayList<String>();
	
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			StringTokenizer wordL = new StringTokenizer(value.toString());
			String curr= null;
			
			while (wordL.hasMoreTokens()) {
				curr=wordL.nextToken();
				if (searchDictionary.contains(curr)) {
					word.set(curr);
					context.write(word, one);
				}
			}
		}
	
	protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
		throws IOException, InterruptedException{
		URI[] cacheFile=context.getCacheFiles();
		
		String filename=null;
		int lastindex = cacheFile[0].toString().lastIndexOf('/');
		if(lastindex != -1)
		{
			filename = cacheFile[0].toString().
					substring(lastindex+1,cacheFile[0].toString().length());
		}
		else
		{
			filename=cacheFile[0].toString();
		}
		try {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line=null;
				while((line = reader.readLine())!= null) {
					StringTokenizer listTokens = new StringTokenizer(line);
					String token =null;
					while(listTokens.hasMoreTokens()) {
						token = listTokens.nextToken();
						searchDictionary.add(token);
					}
				}
		reader.close();
		} catch (FileNotFoundException e) {
			System.err.println("Exception opening file" + StringUtils.stringifyException(e));
		} catch (IOException e) {
			System.err.println("Exception reading " + StringUtils.stringifyException(e));
		}
	}
}

	public static class freqWordReducer
	extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWritable totalCount;
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
		int wordCount = 0;
			Iterator<IntWritable> x = values.iterator();
		while (x.hasNext()) {
			wordCount += x.next().get();
		}
		totalCount.set(wordCount);
		context.write(key,totalCount);
	}
}

public static void main(String[] args) throws Exception {
		Job job =Job.getInstance();
		job.addCacheFile(new Path(args[2]).toUri());
		job.setJarByClass(distrFreqCount.class);
		job.setJobName("distributed frequency count");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(freqWordMapper.class);
		job.setReducerClass(freqWordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
