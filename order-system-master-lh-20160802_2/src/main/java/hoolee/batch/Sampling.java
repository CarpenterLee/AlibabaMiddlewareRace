package hoolee.batch;

import hoolee.sort.IntervalSampler;
import hoolee.sort.Sampler;
import hoolee.sort.SplitSampler;
import hoolee.util.DistinctDisk;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Sampling {
	public ArrayList<Sampler> sampleAll_Split(File[] inFiles, 
			final double freq,
			final int assumed_line_size_in_B, 
			int threadPoolSize) throws Exception{
		System.out.println("###method sampleAll_Split()");
		System.out.println("freq=" + freq);
		System.out.println("assumed_line_size_in_B=" + assumed_line_size_in_B);
		System.out.println("threadPoolSize=" + threadPoolSize);
		final long total_file_size = totalFileSize(inFiles);
		int assumed_total_lines_in_file = (int)(total_file_size/assumed_line_size_in_B);
		final int total_lines_to_sample = (int)(assumed_total_lines_in_file*freq);
		System.out.println("total_file_size=" + total_file_size);
		System.out.println("assumed_total_lines_in_file=" + assumed_total_lines_in_file);
		System.out.println("total_lines_to_sample=" + total_lines_to_sample);
		
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		CompletionService<Sampler> completionService = 
				new ExecutorCompletionService<Sampler>(executor);
		final Object[] disk_read_locks = DistinctDisk.newLocks();
		for(final File inFile : inFiles){
			completionService.submit(new Callable<Sampler>(){
				@Override
				public Sampler call() throws Exception{
					int lines_to_sample = (int)(total_lines_to_sample*inFile.length()/total_file_size)+1;
					Sampler smapler = new SplitSampler(inFile, lines_to_sample, assumed_line_size_in_B);
					synchronized(DistinctDisk.getLockOfFile(disk_read_locks, inFile)){
						System.out.println("SplitSampler, sample file: " + inFile);
						smapler.doSample();
					}
					return smapler;
				}
			});
		}
		ArrayList<Sampler> samplers = new ArrayList<Sampler>();
		for(int i=0; i<inFiles.length; i++){
			Sampler sampler = completionService.take().get();
			samplers.add(sampler);
		}
		executor.shutdownNow();
		return samplers;
	}
	
	public ArrayList<Sampler> sampleAll_Interval(File[] inFiles,
			final double freq, int threadPoolSize) throws Exception{
		final Object[] disk_read_locks = DistinctDisk.newLocks();
		
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		CompletionService<Sampler> completionService = 
				new ExecutorCompletionService<Sampler>(executor);
		for(final File inFile : inFiles){
			completionService.submit(new Callable<Sampler>(){
				@Override
				public Sampler call() throws Exception{
					Sampler sampler = new IntervalSampler(inFile, freq);
					synchronized(DistinctDisk.getLockOfFile(disk_read_locks, inFile)){
						System.out.println("IntervalSampler, sample file: " + inFile);
						sampler.doSample();
					}
//					Object lock = DistinctDisk.getLockOfFile(disk_read_locks, inFile);
//					Sampler2 sampler = new Sampler2(inFile, freq, lock);
//					sampler.doSample_WithLock();
					return sampler;
				}
			});
		}
		ArrayList<Sampler> samplers = new ArrayList<Sampler>();
		for(int i=0; i<inFiles.length; i++){
			Sampler sampler = completionService.take().get();
			samplers.add(sampler);
		}
		executor.shutdownNow();
		return samplers;
	}
	private long totalFileSize(File[] inFiles){
		long all_size = 0;
		for(File f : inFiles){
			all_size += f.length();
		}
		return all_size;
	}

}
