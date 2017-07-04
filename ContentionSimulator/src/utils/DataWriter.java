package utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import simulator.ContentionGenerator;
import model.Job;
import model.Peer;
import model.Task;
import model.User;

public class DataWriter {;	
	
	public static void main(String[] args) throws FileNotFoundException {
		
		Properties props = new Properties();
		FileInputStream input = new FileInputStream(args[0]);
		try {
			props.load(input);
		} catch (IOException e) {
			System.out.println("Error while loading properties!");
			e.printStackTrace();
		}
				
		int nUsers = Integer.parseInt(props.getProperty(ContentionGenerator.USERS));
		int nPeers = Integer.parseInt(props.getProperty(ContentionGenerator.PEERS));
		
		String baseFolder = props.getProperty(ContentionGenerator.BASE_FOLDER);
		String experiment = props.getProperty(ContentionGenerator.EXPERIMENT_NAME);
		String inputFolder = experiment+nPeers+"spt_"+nUsers+"ups/";
		
		String pathBase = baseFolder+inputFolder;
		int [] traces = new int [] {1, 2, 3, 4, 10, 11};		
		String files[] = new String[6];
		for(int i = 0; i<traces.length; i++)
			files[i] = pathBase+experiment+nPeers+"spt_"+nUsers+"ups_gwa-t"+traces[i]+".txt";
		
		ContentionGenerator cg = new ContentionGenerator(0,0);
		List<Peer> peers = cg.readWorkloads(files);
		
		String outputFolder = pathBase+props.getProperty(ContentionGenerator.OUTPUT_FOLDER);
		
		String outputFile = outputFolder+"ordered-"+experiment+nPeers+"spt_"+nUsers+"ups.csv";

		DataWriter dw = new DataWriter(peers, outputFile);
		dw.outputOrderedWorkload(); 
		
	}	

	private List<Peer> peers;
	private List<Job> jobs;
	private String outputFile;

	public DataWriter(List<Peer> peers, String outputFile) {
		this.peers = peers;
		this.outputFile = outputFile;
	}

	public void outputOrderedWorkload() {
		sortJobs();
		FileWriter writer = this.createHeader();
		writer = writeContention(writer);
		flushFile(writer);
	}

	public void sortJobs() {
		jobs = new ArrayList<Job>();
		for (Peer peer : peers) {
			for (User user : peer.getUsers()) {
				jobs.addAll(user.getJobs());
			}
		}
		Collections.sort(jobs);
	}

	private FileWriter createHeader() {

		FileWriter writer = null;
		try {
			writer = new FileWriter(this.outputFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			writer.append("peerId");
			writer.append(',');
			writer.append("jobId");
			writer.append(',');
			writer.append("submitTime");
			writer.append(',');
			writer.append("runTime");
			writer.append('\n');
		} catch (IOException e) {
			e.printStackTrace();
		}

		return writer;
	}

	private FileWriter writeContention(FileWriter writer) {

		for (Job job : jobs) {
			int taskId = 0;
			for (Task task : job.getTasks()) {
				try {
					writer.append(job.getPeerId()
							+ ","
							+ (job.getUserId() + "-J" + job.getId() + "-T" + taskId)
							+ "," + job.getSubmitTime() + ","
							+ task.getRuntime() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
				taskId++;
			}
		}
		return writer;
	}

	private void flushFile(FileWriter writer) {
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
