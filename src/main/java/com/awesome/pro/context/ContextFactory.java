package com.awesome.pro.context;

import gnu.trove.map.hash.THashMap;

import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Factory class for context builders.
 * @author siddharth.s
 */
public class ContextFactory {

	/**
	 * Root logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(ContextFactory.class);

	/**
	 * Map of context name to context builder instance.
	 */
	private static final Map<String, ContextBuilder> CONTEXTS = new THashMap<>();

	/**
	 * @param name Name of the context. e.g. StagingDBContext,
	 * WarehouseContext etc.
	 * @param configFile Name of the configuration file.
	 * @return Context builder instance.
	 */
	public static final ContextBuilder getContextBuilder(final String name,
			final String configFile) {
		if (!CONTEXTS.containsKey(name)) {
			synchronized (ContextFactory.class) {
				if (!CONTEXTS.containsKey(name)) {
					CONTEXTS.put(name, new CassandraContext(name, configFile));
				}
			}
		}
		return CONTEXTS.get(name);
	}

	/**
	 * @param name Name of the context. e.g. StagingDBContext,
	 * WarehouseContext etc.
	 * @return Context builder instance where the underlying context
	 * is stored in memory rather than off heap locations.
	 */
	public static final ContextBuilder getInMemoryContextBuilder(final String name) {
		if (!CONTEXTS.containsKey(name)) {
			synchronized (ContextFactory.class) {
				if (!CONTEXTS.containsKey(name)) {
					CONTEXTS.put(name, new MemoryContext(name));
				}
				else {
					LOGGER.warn("Instance is already present: " + name);
				}
			}
		}
		else {
			LOGGER.warn("Instance is already present: " + name);
		}

		return CONTEXTS.get(name);
	}

}
