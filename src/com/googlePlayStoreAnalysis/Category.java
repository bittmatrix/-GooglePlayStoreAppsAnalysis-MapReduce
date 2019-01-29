package com.googlePlayStoreAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Category extends Configured implements Tool {

	//Mapper class: Using in-Mapper Combiner by combining all values against one key in Hash Map locally
	protected static class categoryMapper extends Mapper<LongWritable, Text, Text, Text> {


		Map<String, List<String>> apps = new HashMap<>();

		//Map function: combining all values against key inside apps(HashMap). 
		//Storing Category as Key and Type and Price(by concatinating with :) as value)
		@Override 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] input = splitbycomma(value.toString());
			if(apps.containsKey(input[1])) {
				List<String> list = apps.get(input[1]);
				list.add(input[6]+":"+input[7]);
				apps.put(input[1], list);
			} else {
				List<String> list = new ArrayList<>();
				list.add(input[6]+":"+input[7]);
				apps.put(input[1], list);
			}
		}

		// writing all key and values after combining to context
		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			Iterator<Map.Entry<String, List<String>>> appIterator = apps.entrySet().iterator();
			while (appIterator.hasNext()) {
				Map.Entry<String, List<String>> entry = appIterator.next();
				context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
			}
		}
	}

	//Reducer class
	public static class categoryReducer extends Reducer<Text, Text, Text, Text> {
		
		//Reduce function: counting all free apps by spliting each value agains category by ':'
		// and checking if first part is equla to 'Free' else counting it as Paid and app then avaerage price by using second part of splitted string
		@Override 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int freeApps = 0;
			int paidApps = 0;
			double totalPrice = 0d;

			while (values.iterator().hasNext()) {
				Text value = values.iterator().next();
				String v = value.toString().substring(1, value.toString().length()-1);
				String[] pairs = v.split(",");
				for (int i =0; i < pairs.length; i++) {
					if(pairs[i].split(":")[0].trim().equals("Free")) {
						freeApps++;
					} else if(pairs[i].split(":")[0].trim().equals("Paid")) {
						paidApps++;
						totalPrice += Double.valueOf(pairs[i].split(":")[1].substring(1));
					}
				}
				context.write(key, new Text(String.valueOf(freeApps + ", " + paidApps + ", " + (paidApps == 0? "0":new DecimalFormat(".##").format(totalPrice/paidApps)))));
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Category(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
			return -1;
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Category");
		job.setJarByClass(Category.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapperClass(categoryMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(categoryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		int returnValue = job.waitForCompletion(true) ? 0:1;

		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}

		return returnValue;
	}

	// split a line from a csv file into fields, returned as String-array 
	public static String[] splitbycomma(String S) {
		ArrayList<String> L = new ArrayList<String>();
		String[] a = new String[0];
		StringBuffer B = new StringBuffer();
		int i = 0;
		while (i<S.length()) {
			int start = i;
			int end=-1;
			if (S.charAt(i)=='"') { // parse field enclosed in quotes
				B.setLength(0); // clear the StringBuffer
				B.append('"');
				i++;
				while (i<S.length()) {
					if (S.charAt(i)!='"') { // not a quote, just add to B
						B.append(S.charAt(i));
						i++;
					}
					else { // quote, need to check if next character is also quote 
						if (i+1 < S.length() && S.charAt(i+1)=='"') {

							B.append('"');
							i+=2; 
						} else { // no, so the quote marked the end of the String B.append('"');
							L.add(B.toString());
							i+=2;
							break;
						}
					} 
				}
			}
			else { // standard field, extends until next comma
				end = S.indexOf(',',i)-1;
				if (end<0) end = S.length()-1; L.add(S.substring(start,end+1)); i = end+2;
			} 
		}
		return L.toArray(a);
	}

}
