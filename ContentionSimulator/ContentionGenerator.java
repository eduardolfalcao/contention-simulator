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
		String files[] = new String[]{ //"/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t1.txt",
									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t2.txt"//,
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t3.txt",
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t4.txt",
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t10.txt",
//									   "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t11.txt"
									   };
		double peerCapacity = 10;
		int granularity = 50;
		ContentionGenerator cg = new ContentionGenerator(peerCapacity, granularity);
		cg.readWorkloads(files);
		cg.sortJobs();
		
//		for(int i = 0; i<10; i++){
//			for(Task task : cg.getJobs().get(i).getTasks()){
//				System.out.println("Job id: "+cg.getJobs().get(i).getId()+
//						"; SubmitTime: "+cg.getJobs().get(i).getSubmitTime()+
//						"; Runtime: "+task.getRuntime());
//			}
//		}
		
		System.out.println();
		
		cg.fulfillRequested();
		
		//print map
		for (Map.Entry<Integer, Integer> entry : cg.getRequested().entrySet()) {
		    Integer key = entry.getKey();
		    Integer value = entry.getValue();
		    if(key < 50)
		    	System.out.println("["+key+"] == "+value+" requested");
		}
		
	}
	
	private int granularity;
	private double peerCapacity;
	private List<Peer> peers;
	private List<Job> jobs;
	
	private Map<Peer, HashMap<Integer, Integer>> requested;
//	private Map<Integer, Integer> supplied;
	
//	private Map<Integer, Double> contention;
	
	public ContentionGenerator(double peerCapacity, int granularity){
		this.peerCapacity = peerCapacity;
		this.granularity = granularity;
		
		peers = new ArrayList<Peer>();	
		jobs = new ArrayList<Job>();
		
		requested = new HashMap<Peer, HashMap<Integer,Integer>>();
//		supplied = new HashMap<Integer,Integer>();
		
//		contention = new HashMap<Integer, Double>();
	}
	
	public void readWorkloads( String[] files){
		DataReader df = new DataReader();
		for(String file :files){
			System.out.println("Running on file: "+file);
			df.readWorkload(peers, file);
		}
	}
	
	public void sortJobs(){
		for(Peer peer : peers){
			for(User user : peer.getUsers())
				jobs.addAll(user.getJobs());
		}		
		Collections.sort(jobs);		
	}
	
	public void fulfillRequested(){
		
		for(Peer peer : peers){
			for(User user : peer.getUsers())
				jobs.addAll(user.getJobs());
		}	
		
		
		int lastTaskEndTime = 0;
		
		for(Job job : jobs){
			Integer initialKey = job.getSubmitTime()/granularity;
			for(Task task : job.getTasks()){
				int endTime = job.getSubmitTime()+task.getRuntime();
				lastTaskEndTime = (endTime>lastTaskEndTime)? endTime : lastTaskEndTime;
				Integer finalKey = endTime/granularity;
				for(int i = initialKey; i<=finalKey; i++){
					Integer currentValue = requested.get(i);
					if(currentValue==null)
						requested.put(i, 1);
					else
						requested.put(i, currentValue+1);
				}				
			}			
		}
		
		//fulfilling the rest of map that doesn't have any request
		for(int i = 0; i <= lastTaskEndTime/granularity; i++){
			Integer currentValue = requested.get(i);
			if(currentValue==null)
				requested.put(i, 0);
		}		
	}
	
	public void fulfillSupplied(){
		
	}
	
	public List<Peer> getPeers(){
		return peers;
	}
	
	public List<Job> getJobs(){
		return jobs;
	}
	
	public Map<Integer, Integer> getRequested(){
		return requested;
	}

}
