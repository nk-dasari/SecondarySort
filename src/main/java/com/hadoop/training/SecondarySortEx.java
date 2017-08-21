package com.hadoop.training;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


public class SecondarySortEx {

	final static Logger logger = Logger.getLogger(SecondarySortEx.class);
	
	
	public static class SecSortMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			// Name,age,gender,marks
			String[] words = value.toString().split(",");
			String compositeKey = words[2]+","+words[3];
			logger.info("Key:"+compositeKey+" Value:"+value);
			context.write(new Text(compositeKey), value);
		}
	}
	
	public static class SecSortReducer extends Reducer<Text,Text,NullWritable,Text>{
		
		@Override
		public void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException{
			int maxMarks = Integer.MIN_VALUE;
			Text val = new Text();
			for(Text value:values){
					context.write(NullWritable.get(), value);
				}
				
			}
			
		}
		
	
	public static class SecSortPartitioner extends Partitioner<Text,Text>{

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			
			int age = Integer.parseInt(value.toString().split(",")[1]);
			logger.info("Age:"+age);
			if(age < 15){
				return 0;
			}else if (age >= 15 && age <20 ){
				return 1;
			}else{
				return 2;
			}
			
		}
		
	}
	
	public static class SecSortComparator extends WritableComparator{

		protected SecSortComparator(){
			super(Text.class,true);
		}
		@Override
		public int compare(WritableComparable wc1,WritableComparable wc2) {
			
			
			Text key1 = (Text) wc1;
			Text key2 = (Text) wc2;
			Text gender1 = new Text(key1.toString().split(",")[0]);
			Text gender2 = new Text(key2.toString().split(",")[0]);
			
			logger.info("key1:"+key1.toString());
			logger.info("key2:"+key2.toString());
			
			IntWritable marks1 = new IntWritable(Integer.parseInt(key1.toString().split(",")[1]));
			IntWritable marks2 = new IntWritable(Integer.parseInt(key2.toString().split(",")[1]));
                  // compare them
			int cmp = gender1.compareTo(gender2);
			if(cmp !=0){
				return cmp;
			}
			return -1 * marks1.compareTo(marks2);
		  }
	}
	
	public static class SecGrpComparator extends WritableComparator{

		protected SecGrpComparator(){
			super(Text.class,true);
		}
		
		@Override
		public int compare(WritableComparable wc1,WritableComparable wc2) {
			
			Text key1 = (Text) wc1;
			Text key2 = (Text) wc2;
			IntWritable marks1 = new IntWritable(Integer.parseInt(key1.toString().split(",")[1]));
			IntWritable marks2 = new IntWritable(Integer.parseInt(key2.toString().split(",")[1]));
                  // compare them
			int cmp = key1.compareTo(key2);
				return cmp;
		
		  }
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Secondary Sort Example");
		job.setJarByClass(SecondarySortEx.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SecSortMapper.class);
		job.setReducerClass(SecSortReducer.class);
		job.setPartitionerClass(SecSortPartitioner.class);
		job.setSortComparatorClass(SecSortComparator.class);
		job.setGroupingComparatorClass(SecGrpComparator.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
