	//package com.test.wordcount;
//The broader requirements are that all the words in the file need to be 
//tokenized first and then the number of occurrence of each and every word need to be generated as output. 

	
	import java.io.IOException;
	import java.util.*;
	
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapreduce.*;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	//map class extends super class i.e MApper  which takes attributes like
	//LongWritable, Text, Text, IntWritable
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	   //Initialize the one variable with a new IntWritable object, and its internal integer value is set to 1
      private final static IntWritable one = new IntWritable(1);
      //Constructor call: The new keyword allocates memory for a new Text object, and Text()
      //calls the constructor of the Text class, 
      //which initializes the object's state
      private Text word = new Text();

      //map method is called once for each input key-value pair
      //Convert the Text value (a line of text) into a String.
      //Tokenize the String into individual words.
      //For each word, emit a (word, 1) key-value pair using context.write(new Text(word), new IntWritable(1)).
      //map method is declared to throw IOException and InterruptedException to handle potential issues
      //during file operations or thread interruptions during the MapReduce execution.
      public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	  //context is used to write output of Mapper class
         String line = value.toString(); //convert value in mapper to type string
         //eg: this is big data class . here key: byte offset(hexadecimal value, hence type is long writable)
         // value: is this is big data class
         StringTokenizer tokenizer = new StringTokenizer(line);
         //string line is passed to stringtokenizer class
         //tokenizer extracts words on basis of spaces
         //loop to check whether TOkenizer has any more words or not
         while (tokenizer.hasMoreTokens()) {
        	 //word.set is picking next word and assigning value
            word.set(tokenizer.nextToken());
            //write function write we pass key:word and assigning value:one against it
            //eg:this,one 
            //is,one,one
            //big,one
            //Context object acts as an interface for the Mapper
            //write():is a method of the Context object used to emit a key-value pair.
            //one:value associated with the key
            context.write(word, one);
         }
      }
   }
   //reducer class extends main class where input key is text and input value is IntWritable 
  //which is output of mapper 
   
//	//reduce class extends super class i.e reducer  which takes attributes like
	//Text, IntWritable, Text, IntWritable
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//reduce function takes key text, values of type Intwritable and context has final output of mapper
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
      //Iterable is nothing but list of values against particular key
         throws IOException, InterruptedException {
         int sum = 0;//to calculate frequency of the key
         //read every value and keep adding
         for (IntWritable val : values) {
            sum += val.get();
         }
         //loop executes till it reaches end of values in Iterable
         context.write(key, new IntWritable(sum));//write key(word) and count
         //eg:this,(1)
         //is,(2)
         //big,(1) parenthesis signifies list
      }
   }
   
   //main function: driver function
   

   public static void main(String[] args) throws Exception {
	   //create object of class Configuration
      Configuration conf = new Configuration();
//define job that needs to get executed on Hadoop cluster with name of Mapreduce program
      Job job = new Job(conf, "wordcount");

      //setJarByClass() method instructs Hadoop to locate the JAR file
      //that contains the specified class i.e WordCount.class
      job.setJarByClass(WordCount.class);
      //setOutputKeyClass() method sets class of output key for the job
      job.setOutputKeyClass(Text.class);
      //set output value class which is Intwritable
      //sets the Writable class that represents the type of the value
      //output is typically written to a file in HDFS
      job.setOutputValueClass(IntWritable.class);
      //set mapper class defines which Java class will serve as the Mapper for the MapReduce job.
      job.setMapperClass(Map.class);
      //set reducer class:represents the actual Java class that contains the implementation of the Reducer logic
      job.setReducerClass(Reduce.class);

      //set inputformat class 
      //setInputFormatClass():method of the Job class is used to specify how the input data for the MapReduce job
      //should be read and split into records for processing by the mappers.
      job.setInputFormatClass(TextInputFormat.class);
      //set output format class
      //defines how the output of the MapReduce job will be written to the file system.
      job.setOutputFormatClass(TextOutputFormat.class);

      //set input path of input file to be used by mapreduce program
      //args[0] means very first argument passed from command line is the input path
      FileInputFormat.addInputPath(job, new Path(args[0]));
      //similarly output path is argument [1]
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      //while executing word count program If I give
      //hadoop jar wordcount.jar /input /output 
      //args[0] is /input and args[1] is /output

      //waitForCompletion():method of the Job class initiates the job submission process to the Hadoop cluster
      //and then enters a blocking state, continuously monitoring the job's progress.
      job.waitForCompletion(true);
   }

}
