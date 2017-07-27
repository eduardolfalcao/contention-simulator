package utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import simulator.ContentionGenerator;


public class CsvGenerator{
	
	private String outputFile;	
		
	public CsvGenerator(String outputFile){
		this.outputFile = outputFile;
	}
	
	public void outputContention(Map<Integer, Integer> requestedToTheFederation, Map<Integer, Integer> suppliedToTheFederation, int capacity){	
		FileWriter writer = this.createHeaderForContention();
		writer = writeContention(writer, requestedToTheFederation, suppliedToTheFederation, capacity);
		flushFile(writer);
	}
	
	private FileWriter createHeaderForContention(){
		
		FileWriter writer = null;
		try {
			writer = new FileWriter(this.outputFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			 writer.append("t");
			 writer.append(',');
			 writer.append("kappa");
			 writer.append(',');
			 writer.append("capacity");
			 writer.append('\n');
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return writer;	    
	}
	
	private FileWriter writeContention(FileWriter writer, Map<Integer, Integer> requestedToTheFederation, Map<Integer, Integer> suppliedToTheFederation, int capacity){
		
		int size = requestedToTheFederation.size();
		if(size != suppliedToTheFederation.size())
			System.out.println("supplied and requested have different sizes...");
		
		for(int i = 0; i < size; i++){
//			System.out.println("Req: "+contentionGenerator.getRequestedToTheFederation().get(i)+"; Sup: "+contentionGenerator.getSuppliedToTheFederation().get(i)+"; Kappa = "+((double)contentionGenerator.getRequestedToTheFederation().get(i)/contentionGenerator.getSuppliedToTheFederation().get(i)));
			try{
				double requested = (double)requestedToTheFederation.get(i);
				double supplied = (double)suppliedToTheFederation.get(i);
				writer.append(i+","+(requested/supplied)+","+capacity+"\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		
		return writer;
	}
	
	private void flushFile(FileWriter writer){		
	    try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}
