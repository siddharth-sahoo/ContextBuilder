package com.awesome.pro.context;

import gnu.trove.map.hash.THashMap;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * In memory implementation of context builder.
 * @author siddharth.s
 */
public class MemoryContext implements ContextBuilder {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(MemoryContext.class);

	/**
	 * Reference name for the instance.
	 */
	private final String instanceName;

	/**
	 * Map containing all the data.
	 */
	private final Map<String, Map<String, Map<String, String>>> data;

	/**
	 * In memory implementation of context builder.
	 * @param name Reference name of the context instance.
	 * @param configFile Name of the configuration file.
	 */
	// Constructor.
	MemoryContext(final String name, final String configFile) {
		instanceName = name;
		data = new THashMap<>();
		throw new Error("In memory context is not yet implemented.");
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#getName()
	 */
	@Override
	public String getName() {
		return instanceName;
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#addContextNameSpace(java.lang.String)
	 */
	@Override
	public void addContextNameSpace(String name) {
		if (data.containsKey(name)) {
			LOGGER.warn(name + " name space is already present in "
					+ instanceName);
			return;
		}

		synchronized (data) {
			if (!data.containsKey(name)) {
				data.put(name, new THashMap<String, Map<String, String>>());
				LOGGER.info(name + " name space added to " + instanceName);
			} else {
				LOGGER.warn(name + " name space is already present in "
						+ instanceName);
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#addContextualData(java.lang.String, java.lang.String, java.util.Map)
	 */
	@Override
	public void addContextualData(String name, String key,
			Map<String, String> context) {
		if (!data.containsKey(name)) {
			LOGGER.warn(name + " name space not found in " + instanceName);
			return;
		}

		synchronized (data) {
			if (data.containsKey(name)) {
				if (!data.get(name).containsKey(key)) {
					data.get(name).put(key, context);
				}
				else {
					data.get(name).get(key).putAll(context);
				}
			}
			else {
				LOGGER.warn(name + " name space not found in " + instanceName);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#addContextualData(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void addContextualData(String name, String key, String field,
			String value) {
		if (!data.containsKey(name)) {
			LOGGER.warn(name + " name space not found in " + instanceName);
			return;
		}

		synchronized (data) {
			if (data.containsKey(name)) {
				if (!data.get(name).containsKey(key)) {
					data.get(name).put(key, new THashMap<String, String>());
				}
				data.get(name).get(key).put(field, value);
			}
			else {
				LOGGER.warn(name + " name space not found in " + instanceName);
			}
		}
	}

	@Override
	public void addUniqueData(String name, String key, String field, String data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addUniqueData(String name, String key, String field,
			Set<String> data) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#getNamespaces()
	 */
	@Override
	public Set<String> getNamespaces() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#getContextData(java.lang.String)
	 */
	@Override
	public Map<String, Map<String, String>> getContextData(String namespace) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#getContextData(java.lang.String, java.lang.String)
	 */
	@Override
	public Map<String, String> getContextData(String namespace, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#getContextData(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public String getContextData(String namespace, String key, String field) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#getContextDataByField(java.lang.String, java.lang.String)
	 */
	@Override
	public Map<String, String> getContextDataByField(String namespace,
			String field) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.awesome.pro.context.ContextBuilder#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
