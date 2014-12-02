package com.awesome.pro.context;

import java.util.Map;
import java.util.Set;

/**
 * Specifies methods to build contextual data.
 * @author siddharth.s
 */
public interface ContextBuilder extends AutoCloseable {

	/**
	 * @return Name of the context which represents the underlying
	 * data source.
	 */
	String getName();

	/**
	 * Adds a new name space. A name space is a logical categorization of
	 * related contextual data. Names can contain only alphanumeric characters.
	 * @param name Name of the name space.
	 */
	void addContextNameSpace(String name);

	/**
	 * @param name Name of the name space.
	 * @param key Field that is used to connect contextual data when it is
	 * retrieved from multiple sources.
	 * @param context Map of context field to data.
	 */
	void addContextualData(String name, String key, Map<String, String> context);

	/**
	 * @param name Name of the name space.
	 * @param key Field that is used to connect contextual data when it is
	 * retrieved from multiple sources.
	 * @param field Field in the name space to be populated.
	 * @param value Value of the field.
	 */
	void addContextualData(String name, String key, String field, String value);

	/**
	 * @param name Name of the name space.
	 * @param key Field that is used to connect contextual data when it is
	 * retrieved from multiple sources.
	 * @param field Field in the name space to be populated. This will contain
	 * the unique of objects.
	 * @param data A single object to be counted uniquely along with already
	 * added values.
	 */
	void addUniqueData(String name, String key, String field, String data);

	/**
	 * @param name Name of the name space.
	 * @param key Field that is used to connect contextual data when it is
	 * retrieved from multiple sources.
	 * @param field Field in the name space to be populated. This will contain
	 * the unique of objects.
	 * @param data A set of objects to be counted uniquely along with already
	 * added values.
	 */
	void addUniqueData(String name, String key, String field, Set<String> data);

	/**
	 * @return Set of all name spaces created.
	 */
	Set<String> getNamespaces();

	/**
	 * @param namespace Name of the name space from which context data is to be
	 * retrieved.
	 * @return All contextual data stored in the name space. Outer map key is
	 * the connecting id or key. Inner map contains context field name
	 * to respective values.
	 */
	Map<String, Map<String, String>> getContextData(String namespace);

	/**
	 * @param namespace Name space to be queried.
	 * @param key Unique identifier of the context.
	 * @return Context data stored for the key.
	 */
	Map<String, String> getContextData(String namespace, String key);

	/**
	 * @param namespace Name space to be queried.
	 * @param key Unique identifier of the context.
	 * @param field Context field to be retrieved.
	 * @return Value of the context field for the key.
	 */
	String getContextData(String namespace, String key, String field);

	/**
	 * @param namespace Name space to be queried.
	 * @param field Context field to be retrieved.
	 * @return Map of context keys to values for the specified field.
	 */
	Map<String, String> getContextDataByField(String namespace, String field);

}
