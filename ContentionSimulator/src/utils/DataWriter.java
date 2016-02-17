package utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import simulator.ContentionGenerator;
import model.Job;
import model.Peer;
import model.Task;
import model.User;

public class DataWriter {

	public static void main(String[] args) {

		String files[] = new String[] {
				"/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t1.txt",
				"/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t2.txt",
				"/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t3.txt",
				"/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t4.txt",
				"/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t10.txt",
				"/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload_clust_5spt_10ups_gwa-t11.txt" };

		ContentionGenerator cg = new ContentionGenerator(0,0);
		List<Peer> peers = cg.readWorkloads(files);

		String outputFile = "/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/workload/";
		outputFile += "ordered_time_workload_clust_5spt_10ups.csv";

		DataWriter dw = new DataWriter(peers, outputFile);
		dw.outputContention();

	}

	private List<Peer> peers;
	private List<Job> jobs;
	private String outputFile;

	public DataWriter(List<Peer> peers, String outputFile) {
		this.peers = peers;
		this.outputFile = outputFile;
	}

	public void outputContention() {
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
