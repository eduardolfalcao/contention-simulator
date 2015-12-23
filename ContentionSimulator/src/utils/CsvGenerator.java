package utils;

import java.io.FileWriter;
import java.io.IOException;

import simulator.ContentionGenerator;


public class CsvGenerator{
	
	private String outputFile;
	private ContentionGenerator contentionGenerator;	
		
	public CsvGenerator(ContentionGenerator contentionGenerator, String outputFile){
		this.contentionGenerator = contentionGenerator;
		this.outputFile = outputFile;
	}
	
	public void outputContention(){	
		FileWriter writer = this.createHeaderForContention();
		writer = writeContention(writer, contentionGenerator);
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
	
	private FileWriter writeContention(FileWriter writer, ContentionGenerator contentionGenerator){
		
		int size = contentionGenerator.getRequestedToTheFederation().size();
		if(size != contentionGenerator.getSuppliedToTheFederation().size())
			System.out.println("supplied and requested have different sizes...");
		
		for(int i = 0; i < size; i++){
//			System.out.println("Req: "+contentionGenerator.getRequestedToTheFederation().get(i)+"; Sup: "+contentionGenerator.getSuppliedToTheFederation().get(i)+"; Kappa = "+((double)contentionGenerator.getRequestedToTheFederation().get(i)/contentionGenerator.getSuppliedToTheFederation().get(i)));
			try{
				writer.append(i+","+((double)contentionGenerator.getRequestedToTheFederation().get(i)/contentionGenerator.getSuppliedToTheFederation().get(i))+","+contentionGenerator.getPeerCapacity()+"\n");
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
