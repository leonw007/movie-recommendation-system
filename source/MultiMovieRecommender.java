package hw6;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class MultiMovieRecommender {
	
	public static void Convert(String strKey, int[] list) {

		int index_m = strKey.indexOf('#');
		String strMovieId = strKey.substring(0, index_m);	
		list[0] = Integer.parseInt(strMovieId);	//movie ID		
		
		int index_r = strKey.indexOf('r')+1;
		String strRate = strKey.substring(index_r,index_r+1);	
		list[1] = Integer.parseInt(strRate);	//rate value
		
		int index_c = strKey.indexOf('c')+1;
		String strCount = strKey.substring(index_c,strKey.length());	
		list[3] = Integer.parseInt(strCount);	//value value
		
		String strSubKey = strKey.substring(index_m+1);			
		String strType = strSubKey.substring(0, 2);
		
		//System.out.println(strKey);
		//System.out.println(strSubKey);
		//System.out.println(strKey);
		int index_t = 0;
		switch(strType) {
		case "PP":index_t = 0;break;
		case "MR":index_t = 1;break;
//		case "EG":EG(strKey,nCounting,nMovieId);break;
//		case "EA":EA(strKey,nCounting,nMovieId);break;
//		case "EO":EO(strKey,nCounting,nMovieId);break;
		
		case "LG": index_t = 2+LG(strKey,index_c-2);break;
		case "LA": index_t = 4+LA(strKey,index_c-2);break;
		case "LO": index_t = 11+LO(strKey,index_c-2);break;
		}
		list[2] = index_t;
		//System.out.println("Done!");
	}
	
	/*"LG:r*,g*" check*/
	public static int LG(String str,int index_end) {
		int index = str.indexOf('g')+1;
		String strIndex = str.substring(index,index_end);	

		return Integer.parseInt(strIndex);
		
	}
	
	/*"LA:r*,a*" check*/
	public static int LA(String str,int index_end) {
				
		int index = str.indexOf('a')+1;
		String strIndex = str.substring(index,index_end);		
		
		int nAgeIndex=0;
		switch(strIndex) {
		case "1": nAgeIndex = 0;break;
		case "18": nAgeIndex = 1;break;
		case "25": nAgeIndex = 2;break;
		case "35": nAgeIndex = 3;break;
		case "45": nAgeIndex = 4;break;
		case "50": nAgeIndex = 5;break;
		case "56": nAgeIndex = 6;break;
		}
		
		return nAgeIndex;
		
	}
	
	/*"LO:r*,o*" check*/
	public static int LO(String str,int index_end) {
		
		int index = str.indexOf('o')+1;
		String strIndex = str.substring(index,index_end);				
		return Integer.parseInt(strIndex);	

	}
	
	public static class FoFMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text txt = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
//			/*rate::gender::age::occupation*/
			String line = value.toString();
			String[] list = line.split(" ");
			//System.out.println(line);
//			
//			/*Movie ID*/
			String strMovieId = list[0];
//			
//			/*"PP"*/
			txt.set(strMovieId+"#PP");
			context.write(txt, one);
//			
//			/*"LG:r*,g*"*/
			if (list[2].equals("M"))
				txt.set(strMovieId+"#LG:r" + list[1]+ ",g0");
			else if (list[2].equals("F"))
				txt.set(strMovieId+"#LG:r" + list[1]+ ",g1");
			context.write(txt, one);
//			
//			/*"LA:r*,a*"*/
			txt.set(strMovieId+"#LA:r" + list[1]+ ",a"+list[3]);
			context.write(txt, one);
//			
//			/*"LO:r*,o*"*/
			txt.set(strMovieId+"#LO:r" + list[1]+ ",o"+list[4]);
			context.write(txt, one);
//			
//			/*"MR:r*":  movie rate   (r1,r2,r3,r4,r5)*/
			txt.set(strMovieId+"#MR:r" + list[1]);
			context.write(txt, one);
//			
//			/*"EG:g*":  gender       (0/Male,1/Famale)*/
//			if (list[2].equals("M"))
//				txt.set(strMovieId+"#EG:g0");
//			else if (list[2].equals("F"))
//				txt.set(strMovieId+"#EG:g1");
//			context.write(txt, one);
//			
//			/*"EA:a*":  age          (1,18,25,35,45,50,56)*/
//			txt.set(strMovieId+"#EA:a" + list[3]);
//			context.write(txt, one);
//			
//			/*"EO:o*":  occupation	(0~20)*/
//			txt.set(strMovieId+"#EO:o" + list[4]);
//			context.write(txt, one);		
		}
	}
	
	public static class FoFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private Text outtxt = new Text();
		@Override
		protected void reduce(Text txt, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable v: values) {
				sum += v.get();
			}
			//System.out.println(sum);
			String strkey = txt.toString()+",c"+sum;
			outtxt.set(strkey);
			context.write(outtxt, null);
		}
		
	}

	public static class FoFMapper2 extends Mapper<LongWritable, Text, Text, Text>{

		private Text txtkey = new Text();
		private Text txtvalue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			//System.out.println(line);
			int[] list = new int[4]; 
			Convert(line, list);
			
			if(list[2] == 0) {
				for(int rate=1;rate<=5;rate++) {
					
					String strkey = list[0]+","+rate;
					String strvalue = list[2]+","+list[3];
										
					txtkey.set(strkey);
					txtvalue.set(strvalue);
					
					context.write(txtkey, txtvalue);
				}
			}
			else {
				String strkey = list[0]+","+list[1];
				String strvalue = list[2]+","+list[3];
				
				//System.out.println(strkey);
				//System.out.println(strvalue);
				
				txtkey.set(strkey);
				txtvalue.set(strvalue);
				
				context.write(txtkey, txtvalue);		
			}	
		}
	}
	
	public static class FoFReducer2 extends Reducer<Text, Text, Text, IntWritable> {

		private Text txtoutput = new Text();
		
		@Override
		protected void reduce(Text txt, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
						
			String strkey = txt.toString();
			int index = strkey.indexOf(',');
			String strMovie = strkey.substring(0,index);
			String strRate = strkey.substring(index+1,strkey.length());
			
			int movieID = Integer.parseInt(strMovie);
			int rate = Integer.parseInt(strRate);
			
			int[] nvalue = new int[32];
			float[] output = new float[31];
			
//			System.out.println(movieID+" "+rate+" ");
			
			for (Text v: values) {
				//System.out.print(v.toString()+" ");
				strkey = v.toString();
				index = strkey.indexOf(',');
				String strIndex = strkey.substring(0,index);
				String strValue = strkey.substring(index+1,strkey.length());
				int nIndex = Integer.parseInt(strIndex);
				int nValue = Integer.parseInt(strValue);
				nvalue[nIndex] = nValue;
			}
			//System.out.println("\n");
//			for(int i=0;i<31;i++) {
//				System.out.print(nvalue[i]+" ");
//			}
			
			
			for(int i=2;i<31;i++) {
				if(nvalue[1]!=0)
					output[i-2] = (float)nvalue[i]/nvalue[1];
				else
					output[i-2] = 0;
			}
			if(nvalue[0] != 0)
				output[30] = (float)nvalue[1]/nvalue[0];
			else
				output[30] = 0;
			
			String strOutput = movieID+" "+rate;
			for(int i=0;i<31;i++)
				strOutput = strOutput+" "+String.format("%.2f",output[i]);
				//System.out.print(output[i]+" ");
			txtoutput.set(strOutput);
			context.write(txtoutput, null);

		}
	}
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if(args.length != 3) {
			System.err.println("Usage: FoF <in> <temp> <out>");
			return;
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "fof");
		job.setJarByClass(MultiMovieRecommender.class);
		job.setMapperClass(FoFMapper.class);
		job.setReducerClass(FoFReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "fof2");
		job2.setJarByClass(MultiMovieRecommender.class);
		job2.setMapperClass(FoFMapper2.class);
		job2.setReducerClass(FoFReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
	
	}

}