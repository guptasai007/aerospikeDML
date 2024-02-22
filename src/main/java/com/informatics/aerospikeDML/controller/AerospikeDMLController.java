package com.informatics.aerospikeDML.controller;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.informatics.aerospikeDML.service.AerospikeInternalService;

@RestController
@RequestMapping("/aerospike")
public class AerospikeDMLController {

	@Autowired
	private AerospikeInternalService aerospikeService;

	@GetMapping("/all")
	public List<Map<String, Object>> getAllRecords(
			@RequestHeader("host") @NonNull String host,
			@RequestHeader("port") @Nullable String port,
			@RequestHeader("username") @Nullable String username,
			@RequestHeader("password") @Nullable String password,
			@RequestHeader("namespace") @NonNull String namespace,
			@RequestHeader("set") @NonNull String set) {
		return aerospikeService.getAllRecords(host,
				StringUtils.isNotEmpty(port) ? Integer.parseInt(port) : 3000,
				username,
				password, namespace, set);
	}

	@GetMapping("/{pk}")
	public Map<String, Object> getRecordByKey(
			@RequestHeader("host") @NonNull String host,
			@RequestHeader("port") @Nullable String port,
			@RequestHeader("username") @Nullable String username,
			@RequestHeader("password") @Nullable String password,
			@RequestHeader("namespace") @NonNull String namespace,
			@RequestHeader("set") @NonNull String set,
			@PathVariable("pk") String pk) {
		return aerospikeService.getRecord(host,
				StringUtils.isNotEmpty(port) ? Integer.parseInt(port) : 3000,
				username,
				password, namespace, set, pk);
	}

	@PostMapping("/save/{pk}")
	public Map<String, Object> saveRecordByKey(
			@RequestHeader("host") @NonNull String host,
			@RequestHeader("port") @Nullable String port,
			@RequestHeader("username") @Nullable String username,
			@RequestHeader("password") @Nullable String password,
			@RequestHeader("namespace") @NonNull String namespace,
			@RequestHeader("set") @NonNull String set,
			@RequestHeader("ttl") @Nullable String ttl,
			@PathVariable("pk") String pk,
			@RequestBody Map<String, Object> data) {
		aerospikeService.saveRecord(host,
				StringUtils.isNotEmpty(port) ? Integer.parseInt(port) : 3000,
				username,
				password, namespace, set, pk, data,
				StringUtils.isNotEmpty(ttl) ? Integer.parseInt(ttl) : 0);
		return aerospikeService.getRecord(host,
				StringUtils.isNotEmpty(port) ? 3000 : Integer.parseInt(port),
				username,
				password, namespace, set, pk);
	}

	@DeleteMapping("/delete/{pk}")
	public boolean deleteRecordByKey(
			@RequestHeader("host") @NonNull String host,
			@RequestHeader("port") @Nullable String port,
			@RequestHeader("username") @Nullable String username,
			@RequestHeader("password") @Nullable String password,
			@RequestHeader("namespace") @NonNull String namespace,
			@RequestHeader("set") @NonNull String set,
			@PathVariable("pk") String pk) {
		return aerospikeService.deleteRecord(host,
				StringUtils.isNotEmpty(port) ? Integer.parseInt(port) : 3000,
				username,
				password, namespace, set, pk);
	}

	@PatchMapping("/truncate")
	public void truncateSet(@RequestHeader("host") @NonNull String host,
			@RequestHeader("port") @Nullable String port,
			@RequestHeader("username") @Nullable String username,
			@RequestHeader("password") @Nullable String password,
			@RequestHeader("namespace") @NonNull String namespace,
			@RequestHeader("set") @NonNull String set) {
		aerospikeService.truncateSet(host,
				StringUtils.isNotEmpty(port) ? Integer.parseInt(port) : 3000,
				username,
				password, namespace, set);
	}

	@PostMapping("/create-secondary-index")
	public void createSecondaryIndex(
			@RequestHeader("host") @NonNull String host,
			@RequestHeader("port") @Nullable String port,
			@RequestHeader("username") @Nullable String username,
			@RequestHeader("password") @Nullable String password,
			@RequestHeader("namespace") @NonNull String namespace,
			@RequestHeader("set") @NonNull String set,
			@RequestHeader("index") @NonNull String indexName,
			@RequestHeader("bin") @NonNull String binName) {
		aerospikeService.createSecondaryIndex(host,
				StringUtils.isNotEmpty(port) ? Integer.parseInt(port) : 3000,
				username, password, namespace, set, indexName, binName);
	}

	@DeleteMapping("/drop-secondary-index")
	public void deleteSecondaryIndex(
			@RequestHeader("host") @NonNull String host,
			@RequestHeader("port") @Nullable String port,
			@RequestHeader("username") @Nullable String username,
			@RequestHeader("password") @Nullable String password,
			@RequestHeader("namespace") @NonNull String namespace,
			@RequestHeader("set") @NonNull String set,
			@RequestHeader("index") @NonNull String indexName) {
		aerospikeService.deleteSecondaryIndex(host,
				StringUtils.isNotEmpty(port) ? Integer.parseInt(port) : 3000,
				username, password, namespace, set, indexName);
	}

}
