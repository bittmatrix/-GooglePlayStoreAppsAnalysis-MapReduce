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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class Sentiment extends Configured implements Tool {


	//Mapper class
	protected static class AppsMapper extends Mapper<LongWritable, Text, Text, Text> {

		Map<String, List<Float>> apps = new HashMap<>();
		//Map function: for all apps which have Content Rating for 'Everyone' writing 'App Name' as key and Category 
		//after appending 'apps' keyword and tab delimeter as value into context
		@Override 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] input = splitbycomma(value.toString());
			if(input.length == 13) {
				if(input[8].equals("Everyone")) {
					context.write(new Text(input[0]), new Text("apps\t" + input[1]));
				}
			} else {
				if(!input[0].equals("App")) {
					if(apps.containsKey(input[0])) {
						if(!input[3].equals("nan")) {
							List<Float> list = apps.get(input[0]);
							list.add(Float.valueOf(input[3]));
							apps.put(input[0], list);
						}
					} else {
						if(!input[3].equals("nan")) {
							List<Float> list = new ArrayList<>();
							list.add(Float.valueOf(input[3]));
							apps.put(input[0], list);
						}
					}
				}
			}
		}
		
		// writing all key (App Name) and values(Appending 'reviews' keyword at start then tab delimeter 
		// and all valid reviews and then tab delimeter and avg Sentiment value) into to context
		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			Iterator<Map.Entry<String, List<Float>>> appIterator = apps.entrySet().iterator();
			while (appIterator.hasNext()) {
				Map.Entry<String, List<Float>> entry = appIterator.next();
				if(entry.getValue().size() >= 50) {
					double total = 0f;
					double avgSentiment = 0f;
					for(double v: entry.getValue()) {
						total += v;
					}
					avgSentiment = total/entry.getValue().size();
					if (avgSentiment > 0.3) {
						context.write(new Text(entry.getKey()), new Text("reviews\t"+ entry.getValue().size()+ "\t" + new DecimalFormat("#.##").format(avgSentiment)));
					}
				}
			}
		}
	}

	//Reducer class
	public static class AppsReducer extends Reducer<Text, Text, Text, Text> {
		
		//Reduce function: recieving input from both mappers. Splitting string with tab delimeter and checking for first element. 
		// If first element is 'apps' then next element will be category else look for reviews, polarity.
		@Override 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String category = "";
			String reviews = "";
			String polarity = "";
			boolean valid = false;
			for(Text t: values) {
				String parts[] = t.toString().split("\t");
				if(parts[0].equals("apps")) {
					category = parts[1];
				} else if(parts[0].equals("reviews")) {
					reviews = parts[1];
					polarity = parts[2];
					valid = true;
				}
			}
			if(valid)
				context.write(key, new Text(category + ", " + reviews + ", " + polarity));
		}
	}

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Sentiment(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments, input path and output path\n", getClass().getSimpleName());
			return -1;
		}

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Sentiment");
		job.setJarByClass(Sentiment.class);
		job.setMapperClass(AppsMapper.class);
		job.setReducerClass(AppsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs= FileSystem.get(conf); 
		FileStatus[] status_list = fs.listStatus(new Path(args[0]));
		if(status_list != null){
		    for(FileStatus status : status_list){
				FileInputFormat.addInputPath(job, status.getPath());
		    }
		}

		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);

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
