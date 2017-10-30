package oneitemset;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class oneitemset {
	public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 
 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();
 private String T = new String();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens()) {
	 T = itr.nextToken().toString();
     StringTokenizer st = new StringTokenizer(T, ",");
     while (st.hasMoreTokens())
     {  
   	      //Word count here
         word.set(st.nextToken().toString());
         context.write(word, one);
     
     }
    
   }
 }
}
public static ArrayList<String> frequent_one_itemset = new ArrayList<String>();
public static class IntSumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
  
   result.set(sum);
   context.write(key, result);
 
 }
}
public static class IntSumReducer 

    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
   result.set(sum);
   context.write(key, result);
   if(sum>=2) {
     frequent_one_itemset.add(key.toString());
  // context.write(key,null);
   }
 }
}

public static class PruneMapper 
//extends Mapper<Object, Text, Text, IntWritable>{
	extends Mapper<Object, Text, Text, Text>{

private String T = new String();
private String Prune_T = new String();
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString());
while (itr.hasMoreTokens()) {
	Prune_T = "";
 T = itr.nextToken().toString();
 StringTokenizer st = new StringTokenizer(T, ",");
 int flag = 0;
 while (st.hasMoreTokens())
 {  
	 
	String item = st.nextToken().toString();
	for(String string : frequent_one_itemset) {
		if(string.matches(item)) {
			if(Prune_T == "") {
				Prune_T = Prune_T + item;
				
			}
			else {
				Prune_T = Prune_T + "," + item;
				flag++;
			}
			
		}
	}	
 }
  int l = Prune_T.length() - flag;
  context.write(new Text(Integer.toString(l)),new Text(Prune_T));
  //context.write(new Text(Prune_T),null);
}
}
}
public static class PruneReducer 

extends Reducer<Text,Text,Text,Text> {


public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

for (Text val : values) {
  context.write(new Text(val.toString()), null);
}

}
}
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    Path outputPath = new Path(args[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);
    job.setJarByClass(oneitemset.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);

    // Job 2
    
    while(!job.waitForCompletion(true)) {}
    Path prune_output = new Path("hdfs:/ethonwu/prune_output/");
    outputPath.getFileSystem(conf).delete(prune_output, true);
    Job job2 = new Job(conf, "Prune part");
    job2.setJarByClass(oneitemset.class);
    job2.setMapperClass(PruneMapper.class);
    job2.setReducerClass(PruneReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job2, new Path("hdfs:/ethonwu/prune_output/"));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
    
}
