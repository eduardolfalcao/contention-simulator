package simulator;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.DataReader;
import model.Job;
import model.Peer;
import model.Task;
import model.User;

public class ContentionGenerator {
	
	public static void main(String[] args) throws FileNotFoundException {
		String files[] = new String[]{ "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t1.txt"//,
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t2.txt"//,
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t3.txt",
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t4.txt",
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t10.txt",
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t11.txt"
									   };
		int peerCapacity = 10;
		int granularity = 50;
		ContentionGenerator cg = new ContentionGenerator(peerCapacity, granularity);
		cg.readWorkloads(files);
		cg.fulfillRequested();
		
		cg.fulfillRequested();
		
		cg.fulfillFederationRequestsData();
				
		for(int i = 0; i < 100; i++){
			System.out.println("Req: "+cg.getRequestedToTheFederation().get(i)+"; Sup: "+cg.getSuppliedToTheFederation().get(i)+"; Kappa = "+((double)cg.getRequestedToTheFederation().get(i)/cg.getSuppliedToTheFederation().get(i)));			
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
	
	public void readWorkloads( String[] files){
		DataReader df = new DataReader();
		for(String file :files){
			System.out.println("Running on file: "+file);
			df.readWorkload(peers, file);
		}
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
	
	public void fulfillFederationRequestsData(){
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

}