package com.awesome.pro.context;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.awesome.pro.db.cassandra.client.CassandraUtilities;
import com.awesome.pro.db.redis.RedisManager;
import com.awesome.pro.db.redis.client.RedisClientPool;
import com.awesome.pro.db.redis.client.WrappedRedisClient;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

/**
 * Cassandra based implementation of context builder.
 * @author adithya.k
 */
public class CassandraContext implements ContextBuilder {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(
			CassandraContext.class);

	/**
	 * Set of all context names. A context is used to refer
	 * to a set of data related to a single key.
	 * e.g. Interaction context will contain data that can
	 * be connected using interaction IDs.
	 * In Cassandra terms, a name space is a column family.
	 */
	private final Set<String> nameSpace;

	/**
	 * Reference name for the entire context.
	 * Is used to refer to source of data.
	 * In Cassandra terms, it is a key space.
	 */
	private final String contextName;

	/**
	 * Key separator used in deriving unique set name.
	 */
	private static final String KEY_SEPARATOR = "||";

	/**
	 * Cassandra based implementation of context builder.
	 * @param name Name of the context instance.
	 * @param configFile Name of the configuration file.
	 */
	// Constructor.
	public CassandraContext(final String name, final String configFile) {
		CassandraUtilities.createKeyspace(name);
		contextName = name;
		nameSpace = new HashSet<>();
		RedisManager.start(configFile);
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#getName()
	 */
	@Override
	public String getName() {
		return contextName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.awesome.pro.context.IContext#addContextNameSpace(java.lang.String)
	 */
	@Override
	public void addContextNameSpace(final String name) {
		if (!nameSpace.contains(name)) {
			synchronized (CassandraContext.class) {
				if (!nameSpace.contains(name)) {
					CassandraUtilities.createColumnFamily(contextName, name);
					nameSpace.add(name);
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.awesome.pro.context.IContext#addContextualData(java.lang.String,
	 * java.util.Map)
	 */
	@Override
	public void addContextualData(final String name, final String key,
			final Map<String, String> context) {
		CassandraUtilities.storeData(contextName, name, key, context);
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#addContextualData(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void addContextualData(final String name, final String key,
			final String field, final String value) {
		CassandraUtilities.storeData(contextName, name, key, field, value);
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#addUniqueData(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void addUniqueData(final String name, final String key,
			final String field, final String data) {
		if (name == null || key == null || field == null || data == null) {
			LOGGER.warn("Null input provided, ignoring transaction.");
			return;
		}

		final WrappedRedisClient client = RedisClientPool.getRedisClient();
		try {
			client.storeOrAddUniqueData(name + KEY_SEPARATOR + key
					+ KEY_SEPARATOR + field, data);
			addContextualData(name, key, field,
					String.valueOf(client.getUniqueCount(name + KEY_SEPARATOR
							+ key + KEY_SEPARATOR + field)));
		} catch (Exception e) {
			LOGGER.error("Error storing unique data.", e);
		} finally {
			RedisClientPool.returnRedisClient(client);
		}
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#addUniqueData(java.lang.String, java.lang.String, java.lang.String, java.util.Set)
	 */
	@Override
	public void addUniqueData(final String name, final String key,
			final String field, final Set<String> data) {
		if (name == null || key == null || field == null || data == null) {
			LOGGER.warn("Null input provided, ignoring transaction.");
			return;
		}

		final WrappedRedisClient client = RedisClientPool.getRedisClient();
		try {
			client.storeOrAddUniqueData(name + KEY_SEPARATOR + key
					+ KEY_SEPARATOR + field, data);
			addContextualData(name, key, field,
					String.valueOf(client.getUniqueCount(name + KEY_SEPARATOR
							+ key + KEY_SEPARATOR + field)));
		} catch (Exception e) {
			LOGGER.error("Error storing unique data.", e);
		} finally {
			RedisClientPool.returnRedisClient(client);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.awesome.pro.context.IContext#getNamespaces()
	 */
	@Override
	public Set<String> getNamespaces() {
		return nameSpace;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.awesome.pro.context.IContext#getContextData(java.lang.String)
	 */
	@Override
	public Map<String, Map<String, String>> getContextData(
			final String namespace) {
		final Rows<String, String> rows = CassandraUtilities.queryAllRows(
				contextName, namespace);

		final Map<String, Map<String, String>> map =
				new HashMap<String, Map<String, String>>(); 

		final Iterator<Row<String, String>> iter = rows.iterator();
		while (iter.hasNext()) {
			final Row<String, String> row = iter.next();
			final ColumnList<String> columns = row.getColumns();
			final int size = columns.size();
			final Map<String, String> nameVal = new HashMap<>();
			for (int i = 0; i < size; i++) {
				nameVal.put(columns.getColumnByIndex(i).getName(),
						columns.getColumnByIndex(i).getStringValue());
			}
			map.put(row.getKey(), nameVal);
		}

		return map;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.awesome.pro.context.IContext#getContextData(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public Map<String, String> getContextData(final String namespace,
			final String key) {
		final ColumnList<String> columns = CassandraUtilities.queryRow(
				contextName, namespace, key);
		final Map<String, String> nameVal = new HashMap<String, String>();
		final int size = columns.size();

		for (int i = 0; i < size; i++) {
			final Column<String> column = columns.getColumnByIndex(i);
			nameVal.put(column.getName(), column.getStringValue());
		}

		return nameVal;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.awesome.pro.context.IContext#getContextData(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public String getContextData(String namespace, String key, String field) {
		final Column<String> columnValue = CassandraUtilities.
				queryRowByColumn(contextName, namespace, key, field);
		return columnValue.getStringValue();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.awesome.pro.context.IContext#getContextDataByField(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public Map<String, String> getContextDataByField(String namespace,
			String field) {
		final Rows<String, String> rows = CassandraUtilities.queryAllRows(
				contextName, namespace);
		final Map<String, String> keyField = new HashMap<String, String>();
		final Iterator<Row<String, String>> iter = rows.iterator();

		while (iter.hasNext()) {
			final Row<String, String> row = iter.next();
			final ColumnList<String> columns = row.getColumns();
			final int size = columns.size();
			for (int i = 0; i < size; i++) {
				Column<String> column = columns.getColumnByIndex(i);
				if (column.hasValue()
						&& column.getName().equalsIgnoreCase(field)) {
					keyField.put(row.getKey(), column.getStringValue());
				}
				// else continue;
			}
		}

		return keyField;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.awesome.pro.context.ContextBuilder#close()
	 */
	@Override
	public void close() {
		LOGGER.info("Keyspace is being dropped: " + contextName);
		CassandraUtilities.dropKeyspace(contextName);
	}

}
