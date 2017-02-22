/**
 * 
 */
package com.suren.mbr.d2io.config;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.security.auth.login.Configuration;

import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * @author Surendra Myneni
 *
 */
public class TestConfigLoad {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws Exception{

        Yaml yaml = new Yaml();  
        try( InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("d2io.yml") ) {
            D2IOConfig config = yaml.loadAs( in, D2IOConfig.class );
            System.out.println( config.toString() );
        }
	}

}
