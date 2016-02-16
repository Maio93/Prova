import org.apache.spark.api.java.*;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;

import scala.Tuple2;

public class Prova {
  public static void main(String[] args) {
    String logFile = "/home/fabio/Scaricati/itwiki-20150121-wikipedia-links.ttl.gz"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();
    
    JavaRDD<String> file_prefix = sc.textFile("prefix.txt");
    JavaPairRDD<String, String> prefix;
    prefix = file_prefix.mapToPair(new PairFunction<String, String, String>() {
		@Override
		public Tuple2<String, String> call(String s) throws Exception {
			return new Tuple2(Div_Stringa(s)[0],Div_Stringa(s)[1]);
		}
	});
    
    String prova = logData.first().toString();
    System.out.println(prova);
    
    String pezzi[] = prova.split(" ");
    
    for(int i=0; i < 3; i++){
    	extract(prefix, pezzi[i]);
    }
  }
  
  public static String[] Div_Stringa(String s){
	  return s.split(" ");
  }
  
  public static void extract(JavaPairRDD<String, String> prefix, final String value){
	  
	  final String result[] = new String[2];
	  
	  prefix.foreach(new VoidFunction <Tuple2<String, String>>() {
		  public void call(Tuple2<String, String> pre){
			  if(value.contains(pre._2)){
				  result[0] = pre._1.toString();
				  String app = value.substring(pre._2.length() + 1, value.length() - 1);
				  result[1] = app;
				  System.out.println(result[0] + ": " + result[1]);
			  }
				  
		  }
	  });
  }
}
