package oneitemset;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class oneitemset {
	public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 private String T = new String();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens()) {
	 T = itr.nextToken().toString();
     StringTokenizer st = new StringTokenizer(T, ",");
     while (st.hasMoreTokens())
     {  
   	      //Word count here
    	
         context.write(new Text(st.nextToken().toString()),new IntWritable(1));
         
     
     }
    
   }
 }
}

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
  // int support_count = 18000000; //Th=0.8 23102978
 //int support_count = 4620000; //Th=0.2 23102978
  // int support_count=70530; // Th=0.8 retail
  //  int support_count=52897; // Th=0.6 retail
 //    int support_count=35264; // Th=0.4 retail
   //  int support_count=17632; // Th=0.2 retail
   Configuration conf = context.getConfiguration();
	 String temp = conf.get("Support");
	 System.out.println("Catch You!!!: "+temp);
	 int support_count = Integer.parseInt(temp);
   for (IntWritable val : values) {
     sum += val.get();
   }
   //result.set(sum);
   //context.write(key, result);
   
   if(sum>=support_count) {
	   result.set(sum);
	   context.write(key, result);
   //  frequent_one_itemset.add(key.toString());
    
   }
   
 }
}

public static class PruneMapper 

	extends Mapper<Object, Text, Text, Text>{

private String T = new String();
private String Prune_T = new String();
public ArrayList<String> frequent_one_itemset = new ArrayList<String>();

public void setup(Context context) throws IOException{
	Configuration conf = context.getConfiguration();
	 String temp = conf.get("F1");
	 Path pt = new Path(temp+"/part-r-00000");
	 FileSystem fs = FileSystem.get(new Configuration());
	 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	 String line;
	 line=br.readLine();
	 while (line != null){
		 String[] line_split = line.split("\t");
		 frequent_one_itemset.add(line_split[0]);
		 line=br.readLine();
	 }
	// br.close();
	 //fs.close();
}
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	 
   //System.out.println(frequent_one_itemset);
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
	Prune_T = new String();
	Prune_T = "";
 T = itr.nextToken().toString();
 int flag = 0;
 String[] st = T.split(",");
 for(String item :st) {
	 if(frequent_one_itemset.contains(item)) {
			if(Prune_T == "") {
				Prune_T = Prune_T + item;
				
			}
			else {
				Prune_T = Prune_T + "," + item;
				flag++;
			}
			
		}
 }
 
 
 
  int l = Prune_T.length() - flag;
  if(Prune_T.length()==0) { continue;  }
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
    if (otherArgs.length != 4) {
      System.err.println("Usage: wordcount <in><F1out><output><support>");
      System.exit(2);
    }
    
	Date date; 
	long start, end; 
	date = new Date(); start = date.getTime(); 
	 Path outputPath = new Path(args[1]);
	    Path inputPath = new Path(args[0]);
	    Path prune_output = new Path(args[2]);
	 int supp = Integer.parseInt(args[3]);
	    conf.set("Support",Integer.toString(supp));
	    conf.set("F1", outputPath.toString());
    Job job = new Job(conf, "word count");
   
   // Path inputPath = new Path("/ethonwu/dataset/retail.txt");
   // Path inputPath = new Path("/ethonwu/answer.txt");
    //Path outputPath = new Path("/ethonwu/outputbang/");
   // outputPath.getFileSystem(conf).delete(outputPath, true);
   
    job.setJarByClass(oneitemset.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
 //   System.exit(job.waitForCompletion(true) ? 0 : 1);

    // Job 2
    
   // while(!job.waitForCompletion(true)) {}
    job.waitForCompletion(true);
    
    //Path prune_output = new Path("hdfs:/ethonwu/retail_output/");
  //  prune_output.getFileSystem(conf).delete(prune_output, true);
    Job job2 = new Job(conf, "Prune part");
    job2.setJarByClass(oneitemset.class);
    job2.setMapperClass(PruneMapper.class);
    job2.setReducerClass(PruneReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, prune_output);
    //System.exit(job2.waitForCompletion(true) ? 0 : 1);
    job2.waitForCompletion(true);
  /*  Job job3 = new Job(conf, "Find Max len");
    Path Out_Job3 = new Path("/ethonwu/Bang/");
    Out_Job3.getFileSystem(conf).delete(Out_Job3, true);
	job3.setJarByClass(oneitemset.class); 
	job3.setMapperClass(FindLenMapper .class);
	job3.setReducerClass(LenSumReducer.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(IntWritable.class);
	job3.setNumReduceTasks(1);
	FileInputFormat.addInputPath(job3, inputPath);
	FileOutputFormat.setOutputPath(job3,Out_Job3);
    
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  */
    date = new Date(); end = date.getTime();
    
    System.out.printf("Run Time is:%f",(end-start)*0.001F);
    System.exit(0);
    
  }
 
    
}
