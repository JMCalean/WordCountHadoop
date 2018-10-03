import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class doubleWordCount {
public static class doubleMapper
	extends   Mapper<Object,Text,Text,IntWritable>
{
    private Text word=new Text();
    private final static IntWritable one=new IntWritable(1);
    
    public void map(Object key,Text value,Context context)
    		throws IOException, InterruptedException
    {
        StringTokenizer wordL= new StringTokenizer(value.toString());
        String prev=null;
        if (wordL.hasMoreTokens()) {
        	prev=wordL.nextToken();
        }
        String curr=null;
        while (wordL.hasMoreTokens()) {
        curr = wordL.nextToken();
        word.set(prev + " " + curr);
        context.write(word, one);
        prev = curr;
        }
        
    }
}
public static class doubleReducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
    private IntWritable doubleWC=new IntWritable();
    public void reduce(Text key,Iterable<IntWritable> values,Context context)
    		throws IOException, InterruptedException
    {
        int wordCount=0;
        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
        	wordCount += it.next().get();
        }
        	doubleWC.set(wordCount);
        	context.write(key, doubleWC);
    }
}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "double word count");
    job.setJarByClass(doubleWordCount.class);
    job.setMapperClass(doubleMapper.class);
    job.setReducerClass(doubleReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true)? 0 : 1);

}

}