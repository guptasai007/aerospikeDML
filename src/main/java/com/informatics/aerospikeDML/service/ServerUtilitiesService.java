package com.informatics.aerospikeDML.service;

import org.springframework.web.multipart.MultipartFile;

public interface ServerUtilitiesService {

	public String executeCommand(String workingDirectory, String command)
			throws Exception;

	public String runShellScript(String path, String scriptName)
			throws Exception;

	public String uploadFile(String uploadDirectory, MultipartFile file)
			throws Exception;

}
