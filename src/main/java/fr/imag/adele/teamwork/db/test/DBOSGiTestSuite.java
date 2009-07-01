/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package fr.imag.adele.teamwork.db.test;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.apache.felix.ipojo.junit4osgi.OSGiTestCase;
import org.apache.felix.ipojo.junit4osgi.OSGiTestSuite;
import org.osgi.framework.BundleContext;

public class DBOSGiTestSuite extends OSGiTestSuite {
	
	/*
	 * Connection parameters to Test database
	 */
	public int _port = 3306;
	public String _host = "localhost";
	public String _dbName = "TestModelDB";
	private String _pwd = "ertdfg";
	private String _login = "root";
	private String _dbSpecificURLPart = "mysql";
	
	public DBOSGiTestSuite(String testName, BundleContext bc) {
		super(testName, bc);
	}
	
	public DBOSGiTestSuite(Class testClass, BundleContext bc) {
		super(testClass, bc);
	}

	public void setConnectionParams(String dbSpecificURLPart, String host, 
			int port, String dbName, String login, String pwd) {
		_dbSpecificURLPart = dbSpecificURLPart;
		_host = host;
		_port = port;
		_dbName = dbName;
		_login = login;
		_pwd = pwd;
	}
	
	public void addTestSuite(DBOSGiTestSuite suite) {
		suite.setBundleContext(context);
		addTest(suite);
	}

	 /**
     * Adds the tests from the given class to the suite
     */
    public void addTestSuite(Class testClass, String dbSpecificURLPart, String host, 
			int port, String dbName, String login, String pwd) {
    	if (ModelVersionDBTestCase.class.isAssignableFrom(testClass)) {
    		DBOSGiTestSuite testSuite = new DBOSGiTestSuite(testClass, context);
    		testSuite.setConnectionParams(dbSpecificURLPart, host, port, dbName, login, pwd);
            addTest(testSuite);
        } else 
        	super.addTestSuite(testClass);
    }
    
    public void runTest(Test test, TestResult result) {
        if (test instanceof DBOSGiTestSuite) {
            ((OSGiTestSuite) test).setBundleContext(context);
            //((DBOSGiTestSuite) test).setConnectionParams(_dbSpecificURLPart, _host, _port, _dbName, _login, _pwd);
            test.run(result);
        } else if (test instanceof ModelVersionDBTestCase) {
            ((OSGiTestCase) test).setBundleContext(context);
            ((ModelVersionDBTestCase) test).setConnectionParams(_dbSpecificURLPart, _host, _port, _dbName, _login, _pwd);
            test.run(result);
        } else 
        	super.runTest(test, result);

    }
}
