package parallel_final_project;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class BayesFunc extends UDF{
	
	//this is a Hive user defined function which would be invoked in the hive command. 
	//It is used to read data from the hive and return the result of naive bayes classifier to do classification.
	public DoubleWritable evaluate(Text gender, Text age, Text occupation, Text rate_prob){
		
		DoubleWritable prob = new DoubleWritable();
		double tem;
		double g = Double.parseDouble(gender.toString());
		double a = Double.parseDouble(age.toString());
		double o = Double.parseDouble(occupation.toString());
		double r_p = Double.parseDouble(rate_prob.toString());
		tem = g * a * o * r_p;
	
		prob.set(tem);
		return prob; 
	}

}
