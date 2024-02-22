package com.informatics.aerospikeDML.service;

import java.util.List;
import java.util.Map;

public interface AerospikeInternalService {
	public Map<String, Object> getRecord(String host, int port,
			String username,
			String password, String namespace,
			String setName, String pk);

	public List<Map<String, Object>> getAllRecords(String host,
			int port, String username, String password, String namespace,
			String setName);

	public void saveRecord(String host, int port, String username,
			String password,
			String namespace, String setName, String pk,
			Map<String, Object> data, int expiryInSeconds);

	public boolean deleteRecord(String host, int port, String username,
			String password, String namespace, String setName, String pk);

	public void truncateSet(String host, int port, String username,
			String password, String namespace, String setName);

	public void createSecondaryIndex(String host, int port, String username,
			String password, String namespace, String setName, String indexName,
			String binName);

	public void deleteSecondaryIndex(String host, int port, String username,
			String password, String namespace, String setName,
			String indexName);

}
