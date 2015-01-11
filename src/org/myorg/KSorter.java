/*
Copyright (c) 2014 Kanak Mahadik 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/
package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;



public class KSorter 
{
	public static void main(String[] args) throws Exception {

		Configuration conf=new Configuration();
		Job job = new Job(conf,"parallelsort");
   
	
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setPartitionerClass(ScorePartitioner.class);
		
		
		
		job.setJarByClass(KSorter.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		FileInputFormat.setInputPaths(job,  new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));



		job.waitForCompletion(true);
	}
	public static class SortMapper extends Mapper<Text, Text,  FloatWritable, Text> {

		public void map(Text key, Text value,Context context)
		throws IOException, InterruptedException {
			
			
			context.write(new FloatWritable(Float.parseFloat(key.toString())),value);

		}

	}
	public static class ScorePartitioner extends Partitioner<FloatWritable,Text>
	{

		@Override
		public int getPartition(FloatWritable key, Text value, int numReduceTasks) {
			float score=key.get();
			
			 if(numReduceTasks == 0)
	                return 0;
			 if(score >= 1 && score <10)
			 {
				 return 0;
			 }
			 if(score >= 0.1 && score <1)
			 {
				 return 1 % numReduceTasks;
			 }
			 if(score >= 0.01 && score <0.1)
			 {
				 return 2 % numReduceTasks;
			 }
			 if(score >= 0.001 && score < 0.01)
			 {
				 return 3 % numReduceTasks;
			 }
			 if(score >= 0.0001 && score <=0.001)
			 {
				 return 4 % numReduceTasks;
			 }
			 else
				 return 5 % numReduceTasks;
		}
		
	}

	public static class SortReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {


		public void reduce(FloatWritable key, Iterable<Text> value,Context context )
		throws IOException, InterruptedException {
			for(Text val : value) {
				context.write(key,new Text(val));
			}
		}

	}
}
