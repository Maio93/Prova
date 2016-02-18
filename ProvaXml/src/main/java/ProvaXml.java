import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;

public class ProvaXml {

	static List<String> prefix; //ArrayList per contenere i prefissi
	static Map<String, Integer> properties; //HashMap per contenere le proprietà e quante volte vengono utilizzate
	
	public static void main(String[] args) {

		prefix = new ArrayList<String>();
		properties = new HashMap<String, Integer>();
		SparkConf conf = new SparkConf().setAppName("Prova Xml").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile("persona.rdf");//RDD contenete tutti i dati

		
		
		//divido i prefissi dai dati
		JavaRDD<String> dataPrefix = sc.parallelize(logData.take(13));//RDD contenete i prefissi e gli URI		
		logData = logData.subtract(dataPrefix);
		
		//estraggo i prefissi togliendo gli URI
		extract_prefix(dataPrefix);
		
		//conto le proprietà
		count_property(logData);
		
		//stampa risultati
		System.out.println("Ci sono " + properties.size() + " proprietà\n\n");
		System.out.println("Proprietà    N di utilizzi");
		for(String key:properties.keySet()){
			System.out.println(key + ": " + properties.get(key));
		}
		
		sc.close();
	}
	
	/*
	 * metodo che estrae i prefissi
	 * i prefissi sono quelli che vengono preceduti dalla scritta xmlns
	 * escludo il prefisso "rdf" perchè rappresenta i nodi e non le proprietà (credo)
	 */
	public static void extract_prefix(JavaRDD<String> dataPrefix) {
		dataPrefix.foreach(new VoidFunction<String>() {
			public void call(String s) {
				s = s.trim();
					if(s.startsWith("xmlns")){
						String p = s.substring(s.indexOf(":") + 1, s.indexOf("="));
						if(!(p.equals("rdf")))
							prefix.add(p);
					}
			}
		});
	}
	
	/* 
	 * Metodo che prepara le singole stringhe distinguendo
	 * i tag normali ("<"), i tag di chiusura ("</")
	 * e i tag mal indentati, cioè le stringhe che non iniziano con <.
	 * Il tutto viene passato al metodo extract_property che le inserisce in properties
	 */
	public static void count_property(JavaRDD<String> logData) {
		logData.foreach(new VoidFunction<String>() {
			public void call(String s) {
				s = s.trim();
				if(s.startsWith("<") && !(s.startsWith("</"))){
					if(s.endsWith(">"))
						extract_property(s.substring(1, s.length()-1));
					else
						extract_property(s.substring(1));
				}
			}
		});
	}
	
	/*
	 * Metodo che estrae la proprietà e la va inserire in properties
	 * o incrementando il contatore o creando un nuovo campo.
	 * La proprietà si trova all'inizio del tag,
	 * quindi la stringa viene splittata
	 * prima in base a se ci sono altri tag, cioè è presenta (>),
	 * poi in base agli spazi per isolare la proprietà,
	 * infine in base a ":" in modo da riconoscere il prefisso e il nome della proprietà
	 */
	public static void extract_property(String s){
		if(s.contains(">"))
			s = s.split(">")[0];
		
		s = s.split(" ")[0];
		
		String property[] = s.split(":");
		
		if(prefix.contains(property[0])){
			if(properties.containsKey(property[1])){
				int n = properties.get(property[1]);
				n++;
				properties.put(property[1], n);
			}
			else
				properties.put(property[1], 1);
		}
			
	}

}
