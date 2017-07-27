package simulator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import utils.CsvGenerator;
import utils.DataReader;
import model.Job;
import model.Peer;
import model.Task;
import model.User;

public class CapacityCalculator {
	
	public static final String USERS = "users";
	public static final String PEERS = "peers";
	public static final String K = "k";
	public static final String BASE_FOLDER = "base_folder";
	public static final String OUTPUT_FOLDER = "output_folder";
	public static final String EXPERIMENT_NAME = "experiment_name";
	public static final String END_TIME = "end_time";
	public static final String KAPPA = "kappa";
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
		int endTime = Integer.parseInt(props.getProperty(END_TIME));
		
		String baseFolder = props.getProperty(BASE_FOLDER);
		String experiment = props.getProperty(EXPERIMENT_NAME);
		String inputFolder = experiment+nPeers+"spt_"+nUsers+"ups/";
		
		String pathBase = baseFolder+inputFolder;
		int [] traces = new int [] {1, 2, 3, 4, 10, 11};		
		String files[] = new String[6];
		for(int i = 0; i<traces.length; i++)
			files[i] = pathBase+experiment+nPeers+"spt_"+nUsers+"ups_gwa-t"+traces[i]+".txt";
		
		double [] peerKappa = new double [4];
		for(int i = 0; i<peerKappa.length; i++)
			peerKappa[i] = Double.parseDouble(props.getProperty(KAPPA+(i+1)));		
		
		String outputFolder = pathBase+props.getProperty(OUTPUT_FOLDER);
		for(double kappa : peerKappa){
			CapacityCalculator cg = new CapacityCalculator(kappa, granularity, endTime);
			cg.readWorkloads(files);
			cg.fulfillRequested();
			int capacity = cg.fulfillCapacity();
			System.out.println("when kappa = "+kappa+", the capacity should be = "+capacity);
			cg.fulfillFederationRequestedAndSuppliedData(capacity);			
			String outputFile = outputFolder+"peerCapacity"+capacity+"-kappa"+kappa+"-users"+nUsers+"-peers"+nPeers+"-clusters"+nClusters+".csv";
			System.out.println(outputFile);
			CsvGenerator csvGen = new CsvGenerator(outputFile);
			csvGen.outputContention(cg.getRequestedToTheFederation(),cg.getSuppliedToTheFederation(),capacity);
		}    
		
	}
	
	private int granularity;
	private double kappa;
	private int endTimeOfExperiment;
	private List<Peer> peers;
	
	private Map<Peer, HashMap<Integer, Integer>> requestedPerPeer;
	private Map<Integer, Integer> requestedToTheFederation, suppliedToTheFederation;
	
	
	public CapacityCalculator(double kappa, int granularity, int endTime){
		this.kappa = kappa;
		this.granularity = granularity;
		this.endTimeOfExperiment = endTime;
		
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
						if(currentValue==null && i < endTimeOfExperiment)
							requestedPerPeer.get(peer).put(i, 1);
						else if(currentValue!=null && i < endTimeOfExperiment)
							requestedPerPeer.get(peer).put(i, currentValue+1);
					}				
				}			
			}
			
			//fulfilling the rest of map that doesn't have any request
//			for(int i = 0; i <= endTimeOfExperiment; i++){
//				Integer currentValue = requestedPerPeer.get(peer).get(i);
////				if(currentValue==null)
////					requestedPerPeer.get(peer).put(i, 0);
//			}
			
			if(!requestedPerPeer.get(peer).containsKey(0)){
				requestedPerPeer.get(peer).put(0, 0);	
			}			
			requestedPerPeer.get(peer).put(endTimeOfExperiment, 0);
			
		}	
		
		
	}	
	
	public int fulfillCapacity(){
		double capacity = 0;
		int n = 0;
		for(Peer peer : peers){			
			Map<Integer, Integer> timeAndDemand = requestedPerPeer.get(peer);
			
			Entry<Integer, Integer> last = null;			
			for(Entry<Integer, Integer> e : timeAndDemand.entrySet()){
				int period, demand;				
				if(last == null){								
					last = e;
					continue;
				} else{
					demand = last.getValue();
					period = e.getKey() - last.getKey();
					last = e;
				}								
				capacity += period * demand;
				if(demand>0)
					n++;
			}
		}		
		capacity = capacity / (n*kappa); 
		return (int) Math.ceil(capacity);		 
	}
	
	public void fulfillFederationRequestedAndSuppliedData(int peerCapacity){
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

	public double getKappa() {
		return kappa;
	}

}