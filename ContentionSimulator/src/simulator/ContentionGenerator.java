package simulator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import utils.CsvGenerator;
import utils.DataReader;
import model.Job;
import model.Peer;
import model.Task;
import model.User;

public class ContentionGenerator {
	
	public static final String USERS = "users";
	public static final String PEERS = "peers";
	public static final String K = "k";
	public static final String BASE_FOLDER = "base_folder";
	public static final String OUTPUT_FOLDER = "output_folder";
	public static final String EXPERIMENT_NAME = "experiment_name";
	public static final String CAPACITY = "capacity";
	public static final String GRANULARITY = "granularity";	
	
	public static void main(String[] args) throws FileNotFoundException {
		
		Properties props = new Properties();
		FileInputStream input = new FileInputStream(args[0]);
		try {
			props.load(input);
		} catch (IOException e) {
			System.out.println("Error while loading properties!");
			e.printStackTrace();
		}
				
		int nUsers = Integer.parseInt(props.getProperty(USERS));
		int nPeers = Integer.parseInt(props.getProperty(PEERS));
		int nClusters = Integer.parseInt(props.getProperty(K));
		int granularity = Integer.parseInt(props.getProperty(GRANULARITY));
		
		String baseFolder = props.getProperty(BASE_FOLDER);
		String experiment = props.getProperty(EXPERIMENT_NAME);
		String inputFolder = experiment+nPeers+"spt_"+nUsers+"ups/";
		
		String pathBase = baseFolder+inputFolder;
		int [] traces = new int [] {1, 2, 3, 4, 10, 11};		
		String files[] = new String[6];
		for(int i = 0; i<traces.length; i++)
			files[i] = pathBase+experiment+nPeers+"spt_"+nUsers+"ups_gwa-t"+traces[i]+".txt";
		
		int [] peerCapacity = new int [5];
		for(int i = 0; i<peerCapacity.length; i++)
			peerCapacity[i] = Integer.parseInt(props.getProperty(CAPACITY+(i+1)));		
		
		String outputFolder = baseFolder+props.getProperty(OUTPUT_FOLDER);		
		for(int capacity : peerCapacity){
			ContentionGenerator cg = new ContentionGenerator(capacity, granularity);
			cg.readWorkloads(files);
			cg.fulfillRequested();		
			cg.fulfillFederationRequestedAndSuppliedData();			
			String outputFile = outputFolder+"peerCapacity"+capacity+"-users"+nUsers+"-peers"+nPeers+"-clusters"+nClusters+".csv";
			CsvGenerator csvGen = new CsvGenerator(cg, outputFile);
			csvGen.outputContention();
		}    
		
	}
	
	private int granularity;
	private int peerCapacity;
	private List<Peer> peers;
	
	private Map<Peer, HashMap<Integer, Integer>> requestedPerPeer;
	private Map<Integer, Integer> requestedToTheFederation;
	private Map<Integer, Integer> suppliedToTheFederation;
	
	
	public ContentionGenerator(int peerCapacity, int granularity){
		this.peerCapacity = peerCapacity;
		this.granularity = granularity;
		
		peers = new ArrayList<Peer>();	
				
		requestedPerPeer = new HashMap<Peer, HashMap<Integer,Integer>>();
		requestedToTheFederation = new HashMap<Integer,Integer>();
		suppliedToTheFederation = new HashMap<Integer,Integer>();
	}
	
	public List<Peer> readWorkloads(String[] files){
		DataReader df = new DataReader();
		for(String file :files){
			System.out.println("Running on file: "+file);
			df.readWorkload(peers, file);
		}		
		return peers;
	}
		
	public void fulfillRequested(){
		for(Peer peer : peers){
			
			requestedPerPeer.put(peer, new HashMap<Integer, Integer>());	//adding the peer in the hashMap
			
			List<Job> jobsOfApeer = new ArrayList<Job>();
			for(User user : peer.getUsers())
				jobsOfApeer.addAll(user.getJobs());
			
			Collections.sort(jobsOfApeer);							//sorting the jobs by the submit time	
			int lastTaskEndTime = 0;
			for(Job job : jobsOfApeer){
				Integer initialKey = job.getSubmitTime()/granularity;
				for(Task task : job.getTasks()){
					int endTime = job.getSubmitTime()+task.getRuntime();
					lastTaskEndTime = (endTime>lastTaskEndTime)? endTime : lastTaskEndTime;
					Integer finalKey = endTime/granularity;
					for(int i = initialKey; i<=finalKey; i++){
						Integer currentValue = requestedPerPeer.get(peer).get(i);
						if(currentValue==null)
							requestedPerPeer.get(peer).put(i, 1);
						else
							requestedPerPeer.get(peer).put(i, currentValue+1);
					}				
				}			
			}
			
			//fulfilling the rest of map that doesn't have any request
			for(int i = 0; i <= lastTaskEndTime/granularity; i++){
				Integer currentValue = requestedPerPeer.get(peer).get(i);
				if(currentValue==null)
					requestedPerPeer.get(peer).put(i, 0);
			}	
			
		}		
		
	}	
	
	public void fulfillFederationRequestedAndSuppliedData(){
		int lastKey=0;
		
		for (HashMap<Integer,Integer> jobsOfAPeer : requestedPerPeer.values()){
			for(Map.Entry<Integer, Integer> jobsPerGrain : jobsOfAPeer.entrySet()){
				
				lastKey = lastKey<jobsPerGrain.getKey()?jobsPerGrain.getKey():lastKey;
				
				int numberOfRequests = jobsPerGrain.getValue();
				
				if(numberOfRequests<peerCapacity){
					int currentSupplied = suppliedToTheFederation.get(jobsPerGrain.getKey())==null ? 0 : suppliedToTheFederation.get(jobsPerGrain.getKey());
					suppliedToTheFederation.put(jobsPerGrain.getKey(), currentSupplied + peerCapacity - numberOfRequests);
				}
				else if(numberOfRequests>peerCapacity){
					int currentRequested = requestedToTheFederation.get(jobsPerGrain.getKey())==null ? 0 : requestedToTheFederation.get(jobsPerGrain.getKey());
					requestedToTheFederation.put(jobsPerGrain.getKey(), currentRequested + numberOfRequests - peerCapacity);
				}			
			}
		}
		
		for(int i = 0; i <= lastKey; i++){
			if(suppliedToTheFederation.get(i)==null)
				suppliedToTheFederation.put(i, 0);
			if(requestedToTheFederation.get(i)==null)
				requestedToTheFederation.put(i, 0);
		}
	}

	
	public List<Peer> getPeers(){
		return peers;
	}
	
	public Map<Peer, HashMap<Integer, Integer>> getRequestedPerPeer(){
		return requestedPerPeer;
	}
	
	public Map<Integer, Integer> getRequestedToTheFederation(){
		return requestedToTheFederation;
	}
	
	public Map<Integer, Integer> getSuppliedToTheFederation(){
		return suppliedToTheFederation;
	}
	
	public int getGranularity() {
		return granularity;
	}

	public int getPeerCapacity() {
		return peerCapacity;
	}

}