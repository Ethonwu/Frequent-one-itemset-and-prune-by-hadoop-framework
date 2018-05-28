package oneitemset;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
//import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class oneitemset {
	public static class Input_CountMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 private String T = new String();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
   while (itr.hasMoreTokens()) {
	 T = itr.nextToken().toString();
    
    
   	      //Word count here
    	
      context.write(new Text("Count"),new IntWritable(1));
         
     
     
    
   }
 }
}
	public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
 private String T = new String();
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
   while (itr.hasMoreTokens()) {
	 T = itr.nextToken().toString();
     StringTokenizer st = new StringTokenizer(T, " ");
     while (st.hasMoreTokens())
     {  
   	      //Word count here
    	
         context.write(new Text(st.nextToken().toString()),new IntWritable(1));
         
     
     }
    
   }
 }
}
public static class LinesSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 

		 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		   int sum = 0;
		   for (IntWritable val : values) {
		     sum += val.get();
		   }
		  
		 
		   context.write(new Text(Integer.toString(sum)),null);
		 
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
//	 System.out.println("Catch You!!!: "+temp);
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
	// System.out.println("Path: "+pt);
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
	 
//   System.out.println(frequent_one_itemset);
   
	 
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
	Prune_T = new String();
	Prune_T = "";
 T = itr.nextToken().toString();
 int flag = 0;
 String[] st = T.split(" ");
 for(String item :st) {
	 if(frequent_one_itemset.contains(item)) {
			if(Prune_T == "") {
				Prune_T = Prune_T + item;
				
			}
			else {
				Prune_T = Prune_T + " " + item;
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
public static class T_Bit_Map_Mapper 

//extends Mapper<Object, Text, Text, Text>{
extends Mapper<Object, Text, Text, IntWritable>{
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
int[] T_Bit_Map = new int[frequent_one_itemset.size()];
T = itr.nextToken().toString();
int flag = 0;
String[] st = T.split(" ");
for(String item :st) {
	 if(frequent_one_itemset.contains(item)) {
		 T_Bit_Map[frequent_one_itemset.indexOf(item)] = 1;
		 flag++;
	 }
}
//System.out.println(T_Bit_Map.length);
for(int i=0;i<T_Bit_Map.length;i++) {
	Prune_T += Integer.toString(T_Bit_Map[i]);
}

 // context.write(new Text(Integer.toString(flag)), new Text(Prune_T));
  context.write(new Text(Prune_T), new IntWritable(1));
}
}
}
public static class T_Bit_Map_Reducer 

extends Reducer<Text,IntWritable,Text,IntWritable> {


public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {
	sum += val.get();
//context.write(new Text(val.toString()), null);
}
   context.write(key, new IntWritable(sum));
}
}
public static class CountMapper
extends Mapper<Object, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);

public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
   
String T = itr.nextToken();
//System.out.println("Here:"+T);
int sum = 0;
for(int i=0;i<T.split("\t")[0].length();i++)
	sum += Integer.parseInt(Character.toString(T.split("\t")[0].charAt(i)));
   
 //System.out.println("Test:"+T.split("\t")[1]);
 context.write(new Text(Integer.toString(sum)),new IntWritable(Integer.parseInt(T.split("\t")[1])));
}
}
}

public static class CountReducer
extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {
 sum += val.get();
}
result.set(sum);
context.write(key, result);
}
}
public static class FinalMapper
extends Mapper<Object, Text, Text, IntWritable>{


public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
	 
	String T = itr.nextToken();
	int l = Integer.parseInt(T.split("\t")[0]);
	int times = Integer.parseInt(T.split("\t")[1]);
	for(int i=l;i>1;i--) 
		context.write(new Text(Integer.toString(i)), new IntWritable(times));
	
}
}
}

public static class FinalReducer
extends Reducer<Text,IntWritable,Text,IntWritable> {


public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
int sum = 0;
Configuration conf = context.getConfiguration();
String support_temp = conf.get("Support");
int support = Integer.parseInt(support_temp);
for (IntWritable val : values) {
 sum += val.get();
}
    if(sum >=support)
        context.write(key, new IntWritable(sum));
}
}

public static class LinkMapper 

extends Mapper<Object, Text, Text, Text>{

private String T = new String();

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	 Configuration conf = context.getConfiguration();
	 String temp = conf.get("itemset");
	 int l = Integer.parseInt(temp);
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {

T = itr.nextToken().toString();

String[] T_splites = T.split("\t");

if(l==2) {
	
	context.write(new Text("one"), new Text(T_splites[0]));
}
else{
	
	 String[] C= T_splites[0].split(" ");
	 int C_l = C.length;
	 String prefix = "";
	 for(int i=0;i<C_l-1;i++)
	 {
		 if(prefix=="") {
			 prefix = C[i];
		 }
		 else {
			 prefix = prefix + " " + C[i];
		 }
	 }
	 context.write(new Text(prefix), new Text(C[C_l-1]));
	// System.out.println("Prefix is:"+prefix+" and Link item is:"+C[C_l-1]);
	
	
}



}	




}
}

public static class LinkReducer


extends Reducer<Text,Text,Text,Text> {


public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

/*for (Text val : values) {
context.write(new Text(val.toString()), null);
}*/
	
	
  if(key.toString().contains("one")) {
	 
	
	 // List<String> C1 = new ArrayList<String>();
      List<Integer> C1 = new ArrayList<Integer>();
    for(Text val : values) {
    
  	    C1.add(Integer.parseInt(val.toString()));
    	
    }
 //   Arrays.sort(C1);
      Collections.sort(C1);
      
    
    for(int i=0;i<C1.size();i++) {
    	    for(int j=i+1;j<C1.size();j++) {
    	       	String C2="";
    	    	    C2 = Integer.toString(C1.get(i)) + " " + Integer.toString(C1.get(j));
    	    	    context.write(new Text(C2), null);
    	    	    
    	    }
    }
   
    
	  
     
  }
  else {
	 // System.out.println("I'm Here");
	  List<String> C1 = new ArrayList<String>();
	  for(Text val : values) {
			  C1.add(val.toString());
	 }
	  Collections.sort(C1);
	 // System.out.println("YOO"+C1);
	  if(C1.size()!=1) {
		  for(int i=0;i<C1.size();i++) {
	    	    for(int j=i+1;j<C1.size();j++) {
	    	       	String C2="";
	    	    	    C2 = key.toString() +" "+ C1.get(i) + " " + C1.get(j);
	    	    	    context.write(new Text(C2), null);
	    	    	    
	    	    }
	    }
	  }
	  
		  
  }
}
}

public static class C_Mapper 

extends Mapper<Object, Text, Text, Text>{
//extends Mapper<Object, Text, Text, IntWritable>{
private String T = new String();
private String Prune_T = new String();
private int f =0;
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
//br.close();
//fs.close();
}
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//System.out.println(frequent_one_itemset);
	Configuration conf = context.getConfiguration();
	String t = conf.get("Tasks");
StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
while (itr.hasMoreTokens()) {
Prune_T = new String();
Prune_T = "";
int[] T_Bit_Map = new int[frequent_one_itemset.size()];
T = itr.nextToken().toString();
int flag = 0;
String[] st = T.split(" ");
for(String item :st) {
	 if(frequent_one_itemset.contains(item)) {
		 T_Bit_Map[frequent_one_itemset.indexOf(item)] = 1;
		 flag++;
	 }
}
//System.out.println(T_Bit_Map.length);
for(int i=0;i<T_Bit_Map.length;i++) {
	Prune_T += Integer.toString(T_Bit_Map[i]);
}
 f++;
 
//System.out.println("f is :"+f);
 if(f==Integer.parseInt(t))
      f =1;
 context.write(new Text(Integer.toString(f)), new Text(Prune_T));
//context.write(new Text(Prune_T), new IntWritable(1));
}
}
}
public static class C_Reducer 

extends Reducer<Text,Text,Text,Text> {


public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
int sum = 0;
for (Text val : values) {
	//sum += val.get();
  // context.write(key,val);
	context.write(val, null);
}
 //context.write(key, new IntWritable(sum));
}
}
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Usage: wordcount <input><output><support><>Max frequent len");
      System.exit(2);
    }
    int Max_len = 0;
    if(otherArgs.length==4) {
    	   Max_len = Integer.parseInt(args[3]);
    }
	Date date; 
	long start, end; 
	date = new Date(); start = date.getTime(); 
	String OutPath = args[1];
	char detect = OutPath.charAt(OutPath.length()-1);
	if(detect!='/') {
		//System.out.println("YPOO");
		OutPath = OutPath + "/";
		System.out.println(OutPath);
	}
      Path o = new Path(OutPath);
      o.getFileSystem(conf).delete(o, true);
	  Path outputPath = new Path(OutPath+"word_count/");
	  Path inputPath = new Path(args[0]);
	//    Path prune_output = new Path(args[2]);
	 //float min_sup = Float.parseFloat(args[2]);
	  double min_sup = Double.parseDouble(args[2]);
	//  double min_sup = 0.2;
//	  Path inputPath = new Path("/ethonwu/retail.txt");
	   // Path inputPath = new Path("/ethonwu/answer.txt");
	 //   Path outputPath = new Path("/ethonwu/outputbang");
	 // outputPath.getFileSystem(conf).delete(outputPath, true);
	   
	conf.set("F1", outputPath.toString());
	Path tempPath = new Path("/temp");
	
	// Job1 Just Counting how many transactions
	FileSystem lfs = FileSystem.get(conf);
	BufferedReader lbr = new BufferedReader(new InputStreamReader(lfs.open(inputPath)));
	int lines =0;
	while (lbr.readLine() != null) {
        lines++;
    }
	
	lbr.close();
	tempPath.getFileSystem(conf).delete(tempPath, true);
	//Counters count = job1.getCounters();
	//long info = count.findCounter("org.apache.hadoop.mapreduce.Task$Counter","MAP_INPUT_RECORDS").getValue();
    int Transactions = lines;
	double support = (float) Transactions* min_sup;
	int supp = (int)support;
	// Job1 Just Counting how many transactions
	System.out.println("Input Transactions: "+Transactions);
	conf.set("Support",Integer.toString(supp));
    
	//Job Count one-itemset support and Create F1
	
	Job job = new Job(conf, "word count");
    job.setJarByClass(oneitemset.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.waitForCompletion(true);
    
  //Job Count one-itemset support and Create F1
    
   //Job2 Prune original input 
    
    
    Path prune_output = new Path(OutPath+"prune_output/");
    //prune_output.getFileSystem(conf).delete(prune_output, true);
    Job job2 = new Job(conf, "Prune part");
    job2.setJarByClass(oneitemset.class);
    job2.setMapperClass(PruneMapper.class);
    job2.setReducerClass(PruneReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, prune_output);
    job2.waitForCompletion(true);
    
    //Job2 Prune original input 
    
    //Job3 Transfer pruned data to T_Bit_Map
    
    
    Job job3 = new Job(conf, "Construct T_Bit_Map");
    Path Out_Job3 = new Path(OutPath+"T_Bit_Map/");
   // Out_Job3.getFileSystem(conf).delete(Out_Job3, true);
	job3.setJarByClass(oneitemset.class); 
	job3.setMapperClass(T_Bit_Map_Mapper .class);
	job3.setReducerClass(T_Bit_Map_Reducer.class);
	job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(IntWritable.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(IntWritable.class);
	job3.setNumReduceTasks(1);
	FileInputFormat.addInputPath(job3, prune_output);
	FileOutputFormat.setOutputPath(job3,Out_Job3);
    job3.waitForCompletion(true);
    
  
  //Job3 Transfer pruned data to T_Bit_Map
    
  //Job4 Count every Transaction length 
  
    Job job4 = new Job(conf, "Countinng T_Bit_Map");
    Path temp_count = new Path(OutPath+"temp_count/");
   // temp_count.getFileSystem(conf).delete(temp_count, true);
	job4.setJarByClass(oneitemset.class); 
	job4.setMapperClass(CountMapper.class);
	job4.setReducerClass(CountReducer.class);
	job4.setOutputKeyClass(Text.class);
	job4.setOutputValueClass(IntWritable.class);
	job4.setNumReduceTasks(1);
	FileInputFormat.addInputPath(job4, Out_Job3);
	FileOutputFormat.setOutputPath(job4,temp_count);
	job4.waitForCompletion(true);
	
    
  //Job4 Count every Transaction length 
	
  // Job5 Count maximum frequent itemset maybe length
	
	Job job5 = new Job(conf, "Final Countinng T_Bit_Map");
    Path T_Bit_Map_counting = new Path(OutPath+"T_Bit_Map_count_result/");
    //T_Bit_Map_counting.getFileSystem(conf).delete(T_Bit_Map_counting, true);
	job5.setJarByClass(oneitemset.class); 
	job5.setMapperClass(FinalMapper.class);
	job5.setReducerClass(FinalReducer.class);
	job5.setOutputKeyClass(Text.class);
	job5.setOutputValueClass(IntWritable.class);
	job5.setNumReduceTasks(1);
	FileInputFormat.addInputPath(job5, temp_count);
	FileOutputFormat.setOutputPath(job5,T_Bit_Map_counting);
	job5.waitForCompletion(true);
	
	
  // Job5 Count maximum frequent itemset maybe length
    
  // Job6 Generate Candidates to N
	FileSystem c = FileSystem.get(conf);
	BufferedReader cbr = new BufferedReader(new InputStreamReader(c.open(new Path(T_Bit_Map_counting+"/part-r-00000"))));
	String c_line;
	c_line = cbr.readLine();
	int max = Integer.parseInt(c_line.split("\t")[0]);
	int temp = max;
	 while(c_line!=null) {		 		
		// System.out.println("Debug c_line is:"+c_line);		
		 temp = Integer.parseInt(c_line.split("\t")[0]);
		 if(temp>max) {
			 max = temp;
		   }
		 c_line = cbr.readLine();
	 }
	 System.out.println("Max Len is: " + max);
	if(Max_len==0) {
		Max_len = max;
	}
    Path linkinput = outputPath;
  
    int Task = 10;
    
	for(int i=2;i<=Max_len;i++) {
		conf.set("itemset", Integer.toString(i));
		Path CandidatesPath = new Path(OutPath+"Candidates/"+Integer.toString(i)+"/");
		Path T_Bit_MapCandidatesPath = new Path(OutPath+"T_Bit_Map_Candidates/"+Integer.toString(i)+"/");
		Job job6 = new Job(conf, "Candidates "+i+"-itemset");
		job6.setJarByClass(oneitemset.class); 
		job6.setMapperClass(LinkMapper.class);
		job6.setReducerClass(LinkReducer.class);
		
		job6.setMapOutputKeyClass(Text.class);
	    job6.setMapOutputValueClass(Text.class);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		job6.setNumReduceTasks(10);
		FileInputFormat.addInputPath(job6, linkinput);
		FileOutputFormat.setOutputPath(job6,CandidatesPath);
		job6.waitForCompletion(true);
		
		Task = (int)((i+5)/5)*10;
		conf.set("Tasks", Integer.toString(Task));
		Job job7 = new Job(conf, "Candidates to T_Bit_Map"+i+"-itemset");
		job7.setJarByClass(oneitemset.class); 
		job7.setMapperClass(C_Mapper.class);
		job7.setReducerClass(C_Reducer.class);
		job7.setMapOutputKeyClass(Text.class);
	    job7.setMapOutputValueClass(Text.class);
		job7.setOutputKeyClass(Text.class);
		job7.setOutputValueClass(Text.class);
		job7.setNumReduceTasks(Task);
		FileInputFormat.addInputPath(job7, CandidatesPath);
		FileOutputFormat.setOutputPath(job7,T_Bit_MapCandidatesPath);
		job7.waitForCompletion(true);
		
		linkinput = new Path(CandidatesPath.toString());
		
		System.out.println("Now Doing "+i+"-itemset!!");
		//break;
	}
    
    
    date = new Date(); end = date.getTime();
    
    System.out.printf("Run Time is:%f",(end-start)*0.001F);
    System.exit(0);
    
  }
 
    
}
