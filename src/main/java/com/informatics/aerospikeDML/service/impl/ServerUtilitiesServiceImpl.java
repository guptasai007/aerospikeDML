package com.informatics.aerospikeDML.service.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.informatics.aerospikeDML.service.ServerUtilitiesService;

@Service
public class ServerUtilitiesServiceImpl implements ServerUtilitiesService {

	@Value("${command.output.max.lines}")
	private int commandOutputMaxLines;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ServerUtilitiesServiceImpl.class);

	@Override
	public String executeCommand(String workingDirectory, String command)
			throws Exception {
		String finalOutput = "";

		ProcessBuilder commandProcessBuilder = new ProcessBuilder("bash", "-c",
				"cd " + workingDirectory + " && " + command);
		Process commandProcess = commandProcessBuilder.start();
		int exitCode = commandProcess.waitFor();
		String commandOutput = getCommandOutput(commandProcess);
		finalOutput += "Executed command " + command + " with exit code: "
				+ exitCode
				+ " on server\nCommand output (truncated to top "
				+ commandOutputMaxLines + " lines):\n"
				+ commandOutput + "\n";

		LOGGER.info(finalOutput);

		return finalOutput;
	}

	@Override
	public String runShellScript(String path, String scriptName)
			throws Exception {
		String finalOutput = "";
		Process process = Runtime.getRuntime().exec("sh " + path + scriptName);
		LOGGER.info("Triggered script {} on server", path + scriptName);
		String scriptOutput = getCommandOutput(process);
		finalOutput += "Script output (truncated to top "
				+ commandOutputMaxLines + " lines):\n" + scriptOutput;
		LOGGER.info(finalOutput);
		return finalOutput;
	}

	private String getCommandOutput(Process process) throws IOException {
		String output = "";
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(process.getInputStream()));
		int lineCount = 0;
		String s = "";
		while ((lineCount < commandOutputMaxLines)
				&& (s = reader.readLine()) != null) {
			output += s + "\n";
			lineCount++;
		}
		return output;
	}

	@Override
	public String uploadFile(String uploadDirectory, MultipartFile file)
			throws Exception {
		String filePath = uploadDirectory + file.getOriginalFilename();
		File dest = new File(filePath);
		file.transferTo(dest);
		String finalOutput = "File " + filePath + " uploaded successfully.";
		LOGGER.info(finalOutput);
		return finalOutput;
	}

}
