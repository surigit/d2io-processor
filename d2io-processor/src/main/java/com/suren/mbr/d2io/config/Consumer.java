/**
 * 
 */
package com.suren.mbr.d2io.config;

import java.util.Map;

/**
 * @author Surendra Myneni
 *
 */
public class Consumer {
	private String clazz;
	private Map<String, String> cfgMap;
	public String getClazz() {
		return clazz;
	}
	public void setClazz(String clazz) {
		this.clazz = clazz;
	}
	public Map<String, String> getCfgMap() {
		return cfgMap;
	}
	public void setCfgMap(Map<String, String> cfgMap) {
		this.cfgMap = cfgMap;
	}
	@Override
	public String toString() {
		return "Consumer [clazz=" + clazz + ", cfgMap=" + cfgMap + "]";
	}
	
}
