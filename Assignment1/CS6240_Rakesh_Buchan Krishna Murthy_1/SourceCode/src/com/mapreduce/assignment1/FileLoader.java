package com.mapreduce.assignment1;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class FileLoader {

	// List<String> containing the lines of the file.
	static ArrayList<String> inputData = new ArrayList<String>();

	public static void main(String[] args) {

		System.out.println("Enter Input Filename with path:\n");
		Scanner scan = new Scanner(System.in);
		String inputFilePath = scan.nextLine();

		try {

			FileInputStream inputStream = new FileInputStream(inputFilePath);
			GZIPInputStream gzinputStream = new GZIPInputStream(inputStream);
			InputStreamReader inputReader = new InputStreamReader(gzinputStream);
			BufferedReader br = new BufferedReader(inputReader);
			String currentLine = null;
			while ((currentLine = br.readLine()) != null) {
				inputData.add(currentLine);
			}

			// Fetches the number of processors/cores
			int threadCount = Runtime.getRuntime().availableProcessors();

			SequentialExec sequential = new SequentialExec();
			sequential.evaluateAverage();

			NoLockExec noLock = new NoLockExec();
			noLock.initiateProcessing(threadCount);

			CoarseLockExec coarseLock = new CoarseLockExec();
			coarseLock.initiateProcessing(threadCount);

			FineLockExec fineLock = new FineLockExec();
			fineLock.initiateProcessing(threadCount);

			NoSharingExec noSharing = new NoSharingExec();
			noSharing.initiateProcessing(threadCount);

			gzinputStream.close();

		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		} finally {
			scan.close();
		}
	}
}
