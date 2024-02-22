package com.informatics.aerospikeDML.controller;

import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.informatics.aerospikeDML.service.ServerUtilitiesService;

@RestController
@RequestMapping("/server-utils")
public class ServerUtilitiesController {

	@Autowired
	private ServerUtilitiesService serverUtilitiesService;

	@PostMapping("/run-shell-script/{script-name}")
	public String runShellScript(@RequestHeader @NotNull String path,
			@PathVariable("script-name") @NotNull String scriptName)
			throws Exception {
		return serverUtilitiesService.runShellScript(path, scriptName);
	}

	@PostMapping("/execute-command")
	public String executeCommand(@RequestHeader @NotNull String directory,
			@RequestHeader @NotNull String command)
			throws Exception {
		return serverUtilitiesService.executeCommand(directory, command);
	}

	@PostMapping("/upload-file")
	public String uploadFile(
			@RequestHeader("upload-directory") @NotNull String uploadDirectory,
			@RequestParam("file") @NotNull MultipartFile file)
			throws Exception {
		return serverUtilitiesService.uploadFile(uploadDirectory, file);
	}
}

