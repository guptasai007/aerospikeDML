package com.informatics.aerospikeDML.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.informatics.aerospikeDML.service.AerospikeInternalService;

@Service
public class AerospikeInternalServiceImpl implements AerospikeInternalService {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AerospikeInternalServiceImpl.class);

	@Override
	public Map<String, Object> getRecord(String host, int port, String username,
			String password, String namespace, String setName, String pk) {
		LOGGER.info("fetching data in set {} for PK {}", setName, pk);
		try {
			Key key = new Key(namespace, setName, pk);
			AerospikeClient client = getAerospikeClient(host, port, username,
					password);
			Policy policy = client.getReadPolicyDefault();
			policy.sendKey = true;
			Record record = client.get(policy, key);
			if (record == null || record.bins.isEmpty()) {
				LOGGER.info("no record found for PK {} in aerospike set {}", pk,
						setName);
				return null;
			} else {
				LOGGER.info("data fetched: {}", record.bins);
				Map<String, Object> recordMap = new HashMap<>();
				recordMap.put("pk",
						key.userKey != null ? key.userKey.getObject() : null);
				recordMap.putAll(record.bins);
				recordMap.put("ttl", record.getTimeToLive());
				return recordMap;
			}
		} catch (Exception e) {
			LOGGER.error("Error while fetching data to aerospike : ", e);
			throw e;
		}
	}

	@Override
	public void saveRecord(String host, int port, String username,
			String password, String namespace, String setName, String pk,
			Map<String, Object> data, int expiryInSeconds) {
		LOGGER.info(
				"saving data in set {} for PK {} and data {} with expiration {}",
				setName, pk, data, expiryInSeconds);
		try {
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = expiryInSeconds;
			writePolicy.sendKey = true;
			Key key = new Key(namespace, setName, pk);
			Bin[] bins = new Bin[data.size()];
			int counter = 0;
			for (Entry<String, Object> entry : data.entrySet()) {
				if (entry.getKey().length() > 14) {
					LOGGER.info(" KEY {}", entry.getKey());
				}
				Bin bin = new Bin(entry.getKey(), entry.getValue());
				bins[counter++] = bin;
			}
			AerospikeClient client = getAerospikeClient(host, port, username,
					password);
			client.put(writePolicy, key, bins);

			LOGGER.info(
					"saved data in set {} for PK {} and data {} with expiration {}",
					setName, pk, data, expiryInSeconds);
		} catch (AerospikeException e) {
			LOGGER.error("Error while saving data to aerospike {}", e);
			throw e;
		}
	}

	@Override
	public List<Map<String, Object>> getAllRecords(String host, int port,
			String username, String password, String namespace,
			String setName) {
		List<Map<String, Object>> allRecordMaps = new ArrayList<>();
		LOGGER.info("fetching all data in set {}", setName);
		try {
			AerospikeClient client = getAerospikeClient(host, port, username,
					password);
			ScanPolicy scanPolicy = client.getScanPolicyDefault();
			scanPolicy.sendKey = true;
			client.scanAll(scanPolicy, namespace, setName,
					(key, record) -> {
						Map<String, Object> recordMap = new HashMap<>();
						recordMap.put("pk",
								key.userKey != null
										? key.userKey.getObject()
										: null);
						recordMap.putAll(record.bins);
						recordMap.put("ttl", record.getTimeToLive());
						allRecordMaps.add(recordMap);
					});

		} catch (Exception e) {
			LOGGER.error("Error while fetching data to aerospike : ", e);
			throw e;
		}

		return allRecordMaps;
	}

	@Override
	public boolean deleteRecord(String host, int port, String username,
			String password, String namespace, String setName, String pk) {
		LOGGER.info("deleting data from set {} for PK {}", setName, pk);
		try {
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.sendKey = true;
			Key key = new Key(namespace, setName, pk);
			AerospikeClient client = getAerospikeClient(host, port, username,
					password);
			boolean deleted = client.delete(writePolicy,
					key);
			return deleted;
		} catch (Exception e) {
			LOGGER.error("Error while fetching data to aerospike : ", e);
			throw e;
		}
	}

	@Override
	public void truncateSet(String host, int port, String username,
			String password, String namespace, String setName) {
		LOGGER.info("truncating set {}", setName);
		try {
			AerospikeClient client = getAerospikeClient(host, port, username,
					password);
			client.truncate(client.getInfoPolicyDefault(), namespace, setName,
					null);
		} catch (Exception e) {
			LOGGER.error("Error while fetching data to aerospike : ", e);
			throw e;
		}

	}

	private AerospikeClient getAerospikeClient(String host, int port,
			String username, String password) {
		ClientPolicy policy = new ClientPolicy();
		return new AerospikeClient(policy, new Host[]{new Host(host, 3000)});
	}

	@Override
	public void createSecondaryIndex(String host, int port, String username,
			String password, String namespace, String setName, String indexName,
			String binName) {
		LOGGER.info("creating secondary index {} on bin {}, set {}", indexName,
				binName, setName);
		try {
			AerospikeClient client = getAerospikeClient(host, port, username,
					password);
			IndexTask indexTask = client.createIndex(null, namespace, setName,
					indexName, binName, IndexType.STRING);
			if (indexTask.isDone())
				LOGGER.info(
						"secondary index {} creation successful on bin {}, set {}",
						indexName, binName, setName);
		} catch (Exception e) {
			LOGGER.error("Error while creating secondary index in aerospike : ",
					e);
			throw e;
		}
	}

	@Override
	public void deleteSecondaryIndex(String host, int port, String username,
			String password, String namespace, String setName,
			String indexName) {
		LOGGER.info("deleting secondary index {} on set {}", indexName,
				setName);
		try {
			AerospikeClient client = getAerospikeClient(host, port, username,
					password);
			IndexTask indexTask = client.dropIndex(null, namespace, setName,
					indexName);
			if (indexTask.isDone())
				LOGGER.info("secondary index {} deletion successful on set {}",
						indexName, setName);
		} catch (Exception e) {
			LOGGER.error("Error while deleting secondary index in aerospike : ",
					e);
			throw e;
		}
	}

}
