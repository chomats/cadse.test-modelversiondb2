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

import fr.imag.adele.teamwork.db.*;

import org.hsqldb.Server;
import org.osgi.framework.ServiceReference;
import org.apache.felix.ipojo.junit4osgi.OSGiTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ModelVersionDBTestCase extends OSGiTestCase {
	
	private ModelVersionDBService m_db;
	private ServiceReference m_sr;
	
	public void setUp() throws TransactionException {
		if (m_sr == null) {
			m_sr = context.getServiceReference(ModelVersionDBService.class.getName());
		}
		if ((m_sr != null) && (m_db == null)) {
			m_db = (ModelVersionDBService) context.getService(m_sr);
			m_db.setConnectionURL(_url, _login, _pwd);
			try {
				m_db.clear();
			} catch (ModelVersionDBException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void tearDown() {
		if (m_db != null) {
			try {
				if (m_db.isConnected())
					m_db.clear();
			} catch (ModelVersionDBException e) {
				// ignore it
				e.printStackTrace();
			}
		}
		
		if (m_sr != null)
			context.ungetService(m_sr);
		m_db = null;
	}
	
	/*
	 * Constants used for tests
	 */
	private static final String VAL1 = "Value1";
	private static final String VAL2 = "Value2";
	private static final String VAL3 = "Value3";
	private static final String NEW_ATTR_VALUE = "New value";
	private static final String NEW_NEW_ATTR_VALUE = "New New value";
	private static final String FIRST_ATTRIBUTE_VALUE = "first_attribute_value";
	private static final String ATTR8 = "attr8";
	private static final String ATTR7 = "attr7";
	private static final String ATTR6 = "attr6";
	private static final String ATTR5 = "attr5";
	private static final String ATTR4 = "attr4";
	private static final String ATTR3 = "attr3";
	private static final String ATTR2 = "attr2";
	private static final String ATTR1 = "attr1";
	
	/**
	 * UUID of objects, links, object types and link types
	 */
	
	public final static UUID notExistObjId = UUID.randomUUID();
	public final static UUID notExistTypeId = UUID.randomUUID();
	public final static UUID obj1Id = UUID.randomUUID();
	public final static UUID obj2Id = UUID.randomUUID();
	public final static UUID obj3Id = UUID.randomUUID();
	public final static UUID obj4Id = UUID.randomUUID();
	
	public final static UUID objType1Id = UUID.randomUUID();
	public final static UUID objType2Id = UUID.randomUUID();
	
	public final static UUID notExistLinkId = UUID.randomUUID();
	public final static UUID link1SrcId = UUID.randomUUID();
	public final static UUID link1DestId = UUID.randomUUID();
	public final static UUID link2SrcId = UUID.randomUUID();
	public final static UUID link2DestId = UUID.randomUUID();
	public final static UUID link3SrcId = UUID.randomUUID();
	public final static UUID link3DestId = UUID.randomUUID();
	public final static UUID link4SrcId = UUID.randomUUID();
	public final static UUID link4DestId = UUID.randomUUID();
	
	public final static UUID notExistLinkTypeId = UUID.randomUUID();
	public final static UUID linkType1Id = UUID.randomUUID();
	public final static UUID linkType2Id = UUID.randomUUID();
	public final static UUID linkType3Id = UUID.randomUUID();
	
	/*
	 * Connection parameters to Test database
	 */
	public String _dbSpecificURLPart = "mysql";
	public int _port = 3306;
	public String _host = "localhost";
	public String _dbName = "TestModelDB";
	public String _url = "jdbc:" + _dbSpecificURLPart + "://" + _host + ":" + _port + "/" + _dbName;
	private String _pwd = "ertdfg";
	public String _login = "root";
	
	public void setConnectionParams(String dbSpecificURLPart, String host, 
			int port, String dbName, String login, String pwd) {
		_dbSpecificURLPart = dbSpecificURLPart;
		_host = host;
		_port = port;
		_dbName = dbName;
		_login = login;
		_pwd = pwd;
		if (dbSpecificURLPart.startsWith("oracle"))
			_url = "jdbc:" + _dbSpecificURLPart + ":@//" + _host + ":" + _port + "/" + _dbName;
		else
			_url = "jdbc:" + _dbSpecificURLPart + "://" + _host + ":" + _port + "/" + _dbName;
	}
	
	public void testConnectionMethods() throws ModelVersionDBException {
		/*
		 * create 2 HSQLDB servers
		 */
		String server1Name = "TestDB1";
		int server1Port = 9007;
		Server server1 = createHSQLServer(server1Name, server1Port);
		String server2Name = "TestDB2";
		int server2Port = 9008;
		Server server2 = createHSQLServer(server2Name, server2Port);
		
		try {
			/*
			 * Check connection to database used for these tests
			 */
			assertTrue(m_db.isConnected());
			assertEquals(_url, m_db.getConnectionURL());
			assertEquals(_login, m_db.getLogin());
			
			/*
			 * try to connect to first database without login  
			 */
			String server1URL = getHSQLServerURL(server1Name, server1Port);
			m_db.setConnectionURL(server1URL);
			assertTrue(m_db.isConnected());
			assertEquals(server1URL, m_db.getConnectionURL());
			assertNull(m_db.getLogin());
			m_db.clear();
			m_db.createObject(obj1Id, objType1Id, null, false);
			
			// try to connect to second database without login  
			String server2URL = getHSQLServerURL(server2Name, server2Port);
			m_db.setConnectionURL(server2URL);
			assertTrue(m_db.isConnected());
			assertEquals(server2URL, m_db.getConnectionURL());
			assertNull(m_db.getLogin());
			m_db.clear();
			assertFalse(m_db.objExists(obj1Id));
			m_db.createObject(obj2Id, objType2Id, null, false);
			
			/*
			 * connection with login
			 */
			try {
				m_db.setConnectionURL(server1URL, null, "");
				fail();
			} catch (IllegalArgumentException e) {
				assertFalse(m_db.isConnected());
			}
			
			try {
				m_db.setConnectionURL(server1URL, "sa", null);
				fail();
			} catch (IllegalArgumentException e) {
				assertFalse(m_db.isConnected());
			}
			
			m_db.setConnectionURL(server1URL, "sa", "");
			assertTrue(m_db.isConnected());
			assertEquals(server1URL, m_db.getConnectionURL());
			assertEquals("sa", m_db.getLogin());
			assertTrue(m_db.objExists(obj1Id));
			assertFalse(m_db.objExists(obj2Id));
			
			// connection to second db with login
			m_db.setConnectionURL(server2URL, "sa", "");
			assertTrue(m_db.isConnected());
			assertEquals(server2URL, m_db.getConnectionURL());
			assertEquals("sa", m_db.getLogin());
			assertFalse(m_db.objExists(obj1Id));
			assertTrue(m_db.objExists(obj2Id));
			
			/*
			 * connection with dbType without login
			 */
			m_db.setConnectionURL(ModelVersionDBService.HSQL_IN_MEMORY_TYPE, "localhost", server1Port, server1Name);
			assertTrue(m_db.isConnected());
			assertEquals(server1URL, m_db.getConnectionURL());
			assertNull(m_db.getLogin());
			assertTrue(m_db.objExists(obj1Id));
			assertFalse(m_db.objExists(obj2Id));
			
			m_db.setConnectionURL(ModelVersionDBService.HSQL_IN_MEMORY_TYPE, "localhost", server2Port, server2Name);
			assertTrue(m_db.isConnected());
			assertEquals(server2URL, m_db.getConnectionURL());
			assertNull(m_db.getLogin());
			assertFalse(m_db.objExists(obj1Id));
			assertTrue(m_db.objExists(obj2Id));
			
			/*
			 * connection with dbType with login
			 */
			try {
				m_db.setConnectionURL(ModelVersionDBService.HSQL_IN_MEMORY_TYPE, "localhost", server1Port, server1Name, null, "");
				fail();
			} catch (IllegalArgumentException e) {
				assertFalse(m_db.isConnected());
			}
			
			try {
				m_db.setConnectionURL(ModelVersionDBService.HSQL_IN_MEMORY_TYPE, "localhost", server1Port, server1Name, "sa", null);
				fail();
			} catch (IllegalArgumentException e) {
				assertFalse(m_db.isConnected());
			}
			
			m_db.setConnectionURL(ModelVersionDBService.HSQL_IN_MEMORY_TYPE, "localhost", server1Port, server1Name, "sa", "");
			assertTrue(m_db.isConnected());
			assertEquals(server1URL, m_db.getConnectionURL());
			assertEquals("sa", m_db.getLogin());
			assertTrue(m_db.objExists(obj1Id));
			assertFalse(m_db.objExists(obj2Id));
			
			m_db.setConnectionURL(ModelVersionDBService.HSQL_IN_MEMORY_TYPE, "localhost", server2Port, server2Name, "sa", "");
			assertTrue(m_db.isConnected());
			assertEquals(server2URL, m_db.getConnectionURL());
			assertEquals("sa", m_db.getLogin());
			assertFalse(m_db.objExists(obj1Id));
			assertTrue(m_db.objExists(obj2Id));
			
		} catch (Exception e) {
			e.printStackTrace();
			
			// stop servers
			server1.stop();
			server2.stop();
			
			fail("Unexpected Error: " + e.getMessage());
		}
		
		// cleaning code
		server1.stop();
		server2.stop();
	}
	
	public void testDistributedTransactions() throws ModelVersionDBException {
		/*
		 * create 2 HSQLDB servers
		 */
		String server1Name = "TestDB1";
		int server1Port = 9007;
		Server server1 = createHSQLServer(server1Name, server1Port);
		String server2Name = "TestDB2";
		int server2Port = 9008;
		Server server2 = createHSQLServer(server2Name, server2Port);
		
		try {
			/*
			 * try to connect to first database without login  
			 */
			String server1URL = getHSQLServerURL(server1Name, server1Port);
			m_db.setConnectionURL(server1URL);
			assertTrue(m_db.isConnected());
			assertEquals(server1URL, m_db.getConnectionURL());
			assertNull(m_db.getLogin());
			m_db.clear();
			int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
			m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, VAL1);
			
			// try to connect to second database without login  
			String server2URL = getHSQLServerURL(server2Name, server2Port);
			m_db.setConnectionURL(server2URL);
			assertTrue(m_db.isConnected());
			assertEquals(server2URL, m_db.getConnectionURL());
			assertNull(m_db.getLogin());
			m_db.clear();
			assertFalse(m_db.objExists(obj1Id));
			int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, null, false);
			m_db.setObjectValue(obj2Id, obj2Rev1, ATTR1, VAL1);
			
			// begin transaction
			m_db.beginTransaction();
			assertTrue(m_db.hasTransaction());
			
			// update server 2
			m_db.setObjectValue(obj2Id, obj2Rev1, ATTR1, VAL2);
			
			// update server 1
			m_db.setConnectionURL(server1URL);
			assertTrue(m_db.hasTransaction());
			m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, VAL3);
			
			// rollback transaction
			m_db.rollbackTransaction();
			assertFalse(m_db.hasTransaction());
			
			// check server1  
			assertEquals(VAL1, m_db.getObjectValue(obj1Id, obj1Rev1, ATTR1));
			
			// check server 2
			m_db.setConnectionURL(server2URL);
			assertFalse(m_db.hasTransaction());
			assertEquals(VAL1, m_db.getObjectValue(obj2Id, obj2Rev1, ATTR1));
			
			
			// begin transaction
			m_db.beginTransaction();
			assertTrue(m_db.hasTransaction());
			
			// update server 2
			m_db.setObjectValue(obj2Id, obj2Rev1, ATTR1, VAL2);
			
			// update server 1
			m_db.setConnectionURL(server1URL);
			assertTrue(m_db.hasTransaction());
			m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, VAL3);
			
			// commit transaction
			m_db.commitTransaction();
			assertFalse(m_db.hasTransaction());
			
			// check server1  
			assertEquals(VAL3, m_db.getObjectValue(obj1Id, obj1Rev1, ATTR1));
			
			// check server 2
			m_db.setConnectionURL(server2URL);
			assertFalse(m_db.hasTransaction());
			assertEquals(VAL2, m_db.getObjectValue(obj2Id, obj2Rev1, ATTR1));
			
		} catch (Exception e) {
			e.printStackTrace();
			
			// stop servers
			server1.stop();
			server2.stop();
			
			fail("Unexpected Error: " + e.getMessage());
		}
		
		// cleaning code
		server1.stop();
		server2.stop();
	}
	
	public void testObjExistsWithID() throws ModelVersionDBException {
		
		// null object id
		try {
			m_db.objExists(null);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// Not existing object and link
		boolean objExist = m_db.objExists(notExistObjId);
		assertFalse(objExist);
		
		// Existing object
		int obj1Rev = m_db.createObject(obj1Id, objType1Id, null, false);
		assertTrue(m_db.objExists(obj1Id));
		
		// Existing link and object not exist
		Revision linkRev = m_db.addLink(linkType1Id, obj1Id, obj1Rev, obj1Id, obj1Rev, null);
		assertFalse(m_db.objExists(linkRev.getId()));
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteLink(linkRev.getId());
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetMethodsWhenNoTypeExist() throws ModelVersionDBException {
		/*
		 * No type in base
		 */
		
		// objectExists
		assertFalse(m_db.objExists(notExistObjId));
		
		// objectExists with revision number
		assertFalse(m_db.objExists(notExistObjId, 1));
		
		// getObjects
		m_db.clear();
		Set<UUID> objSet = m_db.getObjects();
			
		Set<UUID> expectSet = new HashSet<UUID>();
		assertEquals(expectSet, objSet);
		
		// getObjects with typeId
		objSet = m_db.getObjects(notExistTypeId);
		
		expectSet = new HashSet<UUID>();
		assertEquals(expectSet, objSet);
		
		// getObjectRevs with stateMap
		List<Revision> revs = m_db.getObjectRevs(notExistTypeId, null, false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// getObjectRevs with attribute value
		revs = m_db.getObjectRevs(notExistTypeId, ATTR1, NEW_ATTR_VALUE, false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// getObjectState
		try {
			m_db.getObjectState(notExistObjId, 1);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getObjectValue
		try {
			m_db.getObjectValue(notExistObjId, 1, ATTR1);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getObjectType
		try {
			m_db.getObjectType(notExistObjId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getObjectRevNbs
		try {
			m_db.getObjectRevNbs(notExistObjId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getLastObjectRevNb
		try {
			m_db.getLastObjectRevNb(notExistObjId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// linkExists
		assertFalse(m_db.linkExists(notExistLinkId));
		
		// linkExists with revision
		assertFalse(m_db.linkExists(notExistLinkId, 1));
		
		// linkExists with source revision and destination revision
		assertFalse(m_db.linkExists(notExistLinkTypeId, notExistObjId, 1, notExistObjId, 1));
		
		// getLinkType
		try {
			m_db.getLinkType(notExistLinkId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}

		// getLinks
		Set<UUID> idSet = m_db.getLinks();
		
		assertNotNull(idSet);
		assertTrue(idSet.isEmpty());
		
		// getLinks with linkTypeId
		List<UUID> idList = m_db.getLinks(notExistLinkTypeId);
		
		assertNotNull(idList);
		assertTrue(idList.isEmpty());
		
		// getLinkId with linkTypeId, source revision and destId
		assertNull(m_db.getLinkId(notExistLinkTypeId, notExistObjId, 1, notExistObjId));
		
		// getOutgoingLinks with typeId and source revision
		revs = m_db.getOutgoingLinks(notExistLinkTypeId, notExistObjId, 1);
		
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// getOutgoingLinks with typeId and source revision
		revs = m_db.getOutgoingLinks(notExistObjId, 1, notExistObjId);
		
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// getLinkState
		try {
			m_db.getLinkState(notExistLinkId, 1);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getLinkValue
		try {
			m_db.getLinkValue(notExistLinkId, 1, ATTR1);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getLinkSrc
		try {
			m_db.getLinkSrc(notExistLinkId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getLinkDest
		try {
			m_db.getLinkDest(notExistLinkId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
		
		// getLinkSrcRevs
		revs = m_db.getLinkSrcRev(notExistLinkTypeId, notExistObjId, 1);

		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// getLinkDestRevs
		revs = m_db.getLinkDestRev(notExistLinkTypeId, notExistObjId, 1);

		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// getLinkRevNbs
		assertEquals(0, m_db.getLinkNumber(notExistLinkTypeId, notExistObjId, 1));
		
		// getLinkRevNbs
		try {
			m_db.getLinkRevNbs(notExistLinkId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}

		// getLastLinkRevNb
		try {
			m_db.getLastLinkRevNb(notExistLinkId);
			fail();
		} catch (IllegalArgumentException e) {
			// test passed
		}
	}
	
	public void testObjExistsWithIdAndRev() throws ModelVersionDBException {
		
		// null object id
		try {
			m_db.objExists(null, 1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// Not existing object and link
		boolean objExist = m_db.objExists(notExistObjId, 1);
		assertFalse(objExist);
		
		try {
			m_db.objExists(notExistObjId, ModelVersionDBService.ALL);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		objExist = m_db.objExists(notExistObjId, ModelVersionDBService.ANY);
		assertFalse(objExist);
		
		objExist = m_db.objExists(notExistObjId, ModelVersionDBService.LAST);
		assertFalse(objExist);
		
		// Existing object
		int obj1Rev = m_db.createObject(obj1Id, objType1Id, null, false);
		assertTrue(m_db.objExists(obj1Id, obj1Rev));
		
		try {
			m_db.objExists(obj1Id, ModelVersionDBService.ALL);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		assertTrue(m_db.objExists(obj1Id, ModelVersionDBService.ANY));
		
		assertTrue(m_db.objExists(obj1Id, ModelVersionDBService.LAST));
		
		// Revision does not exist
		assertFalse(m_db.objExists(obj1Id, obj1Rev + 1));
		
		// Existing link and object not exist
		Revision linkRev = m_db.addLink(linkType1Id, obj1Id, obj1Rev, obj1Id, obj1Rev, null);
		assertFalse(m_db.objExists(linkRev.getId(), linkRev.getRev()));
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteLink(linkRev.getId());
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetObjects() throws ModelVersionDBException {
		// No object in base
		Set<UUID> objSet = m_db.getObjects();
		
		assertNotNull(objSet);
		assertTrue(objSet.isEmpty());
		
		// One object
		m_db.createObject(obj1Id, objType1Id, null, false);
		
		objSet = m_db.getObjects();
		Set<UUID> expectSet = new HashSet<UUID>();
		expectSet.add(obj1Id);
		assertEquals(expectSet, objSet);
		
		// Two object
		m_db.createObject(obj2Id, objType2Id, null, true);
		
		objSet = m_db.getObjects();
		expectSet = new HashSet<UUID>();
		expectSet.add(obj1Id);
		expectSet.add(obj2Id);
		assertEquals(expectSet, objSet);
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetObjectsWithTypeId() throws ModelVersionDBException {
		// null type id
		try {
			m_db.getObjects(null);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// No object in base
		Set<UUID> objSet = m_db.getObjects(objType1Id);
		
		assertNotNull(objSet);
		assertTrue(objSet.isEmpty());
		
		// One object of asked type
		m_db.createObject(obj1Id, objType1Id, null, false);
		
		objSet = m_db.getObjects(objType1Id);
		Set<UUID> expectSet = new HashSet<UUID>();
		expectSet.add(obj1Id);
		assertEquals(expectSet, objSet);
		
		objSet = m_db.getObjects(objType2Id);
		assertNotNull(objSet);
		assertTrue(objSet.isEmpty());
		
		// two object of asked type
		m_db.createObject(obj2Id, objType1Id, null, true);
		
		objSet = m_db.getObjects(objType1Id);
		expectSet = new HashSet<UUID>();
		expectSet.add(obj1Id);
		expectSet.add(obj2Id);
		assertEquals(expectSet, objSet);
		
		// two object of asked type
		// and one object of another type
		m_db.createObject(obj3Id, objType2Id, null, true);
		
		objSet = m_db.getObjects(objType1Id);
		expectSet = new HashSet<UUID>();
		expectSet.add(obj1Id);
		expectSet.add(obj2Id);
		assertEquals(expectSet, objSet);
		
		objSet = m_db.getObjects(objType2Id);
		expectSet = new HashSet<UUID>();
		expectSet.add(obj3Id);
		assertEquals(expectSet, objSet);
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
			m_db.deleteObject(obj3Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testObjectIsType() throws ModelVersionDBException {
		// null object id
		try {
			m_db.isType(null);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// object is not a type
		m_db.createObject(obj1Id, objType1Id, null, false);
		assertFalse(m_db.isType(obj1Id));
		
		// object is a type
		m_db.createObject(obj2Id, objType1Id, null, true);
		assertTrue(m_db.isType(obj2Id));
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}

	public void testGetObjectRevsWithStateMap() throws ModelVersionDBException {
		Map<String, Object> attrMap = new HashMap<String, Object>();
		attrMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		attrMap.put(ATTR2, null);
		attrMap.put(ATTR3, new Integer(1234));
		attrMap.put(ATTR4, new Long(1234567890L));
		attrMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		attrMap.put(ATTR6, new Ser("ser1"));

		// Null type id
		try {
			m_db.getObjectRevs(null, attrMap, false);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 

		// No object in base
		List<Revision> revs = m_db.getObjectRevs(objType1Id, attrMap, false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		revs = m_db.getObjectRevs(objType1Id, attrMap, true);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());

		/*
		 * One object 
		 */
		
		// One object revision
		// full state
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, attrMap, false);
		
		revs = m_db.getObjectRevs(objType1Id, attrMap, false);
		List<Revision> expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		assertEquals(expectRevs, revs);
		
		// partial state
		Map<String, Object> attrMap2 = new HashMap<String, Object>();
		attrMap2.put(ATTR2, null);
		attrMap2.put(ATTR4, new Long(1234567890L));
		attrMap2.put(ATTR6, new Ser("ser1"));
		
		revs = m_db.getObjectRevs(objType1Id, attrMap2, false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		assertEquals(expectRevs, revs);
		
		// bad object type
		revs = m_db.getObjectRevs(objType2Id, attrMap, false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// 2 object revisions
		// full state
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		
		revs = m_db.getObjectRevs(objType1Id, attrMap, false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		assertEquals(expectRevs, revs);
		
		// partial state
		revs = m_db.getObjectRevs(objType1Id, attrMap2, false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		assertEquals(expectRevs, revs);

		/*
		 * two objects 
		 */
		// object 2 has 1 revision
		int obj2Rev1 = m_db.createObject(obj2Id, objType1Id, attrMap, false);
		
		revs = m_db.getObjectRevs(objType1Id, attrMap, false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		assertRevListMatch(expectRevs, revs);
		
		// object 2 has 2 revisions
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		
		revs = m_db.getObjectRevs(objType1Id, attrMap, false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev2));
		assertRevListMatch(expectRevs, revs);
		
		m_db.setObjectValue(obj2Id, obj2Rev2, ATTR1, NEW_ATTR_VALUE);
		
		revs = m_db.getObjectRevs(objType1Id, attrMap, false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		assertRevListMatch(expectRevs, revs);
		
		// retrieve only last revisions that match
		revs = m_db.getObjectRevs(objType1Id, attrMap, true);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		assertRevListMatch(expectRevs, revs);
		
		// unexisting attributes
		Map<String, Object> attrMap4 = new HashMap<String, Object>();
		attrMap4.putAll(attrMap);
		attrMap4.put(ATTR7, UUID.randomUUID());

		revs = m_db.getObjectRevs(objType1Id, attrMap4, false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());

		// invalid type of attribute
		try {
			attrMap4.putAll(attrMap);
			attrMap4.put(ATTR1, UUID.randomUUID());

			m_db.getObjectRevs(objType1Id, attrMap4, false);
			fail();
		} catch (ModelVersionDBException e) {
			// test passed
		}

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetObjectRevsWithAttrVal() throws ModelVersionDBException {
		Map<String, Object> attrMap = new HashMap<String, Object>();
		attrMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		attrMap.put(ATTR2, null);
		attrMap.put(ATTR3, new Integer(1234));
		attrMap.put(ATTR4, new Long(1234567890L));
		attrMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		attrMap.put(ATTR6, new Ser("ser1"));
		
		// Null type id
		try {
			m_db.getObjectRevs(null, ATTR1, attrMap.get(ATTR1), false);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 

		// No object in base
		List<Revision> revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), true);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		/*
		 * One object 
		 */
		
		// One object revision
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, attrMap, false);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), false);
		List<Revision> expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR2, attrMap.get(ATTR2), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR3, attrMap.get(ATTR3), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR4, attrMap.get(ATTR4), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR5, attrMap.get(ATTR5), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR6, attrMap.get(ATTR6), false);
		assertEquals(expectRevs, revs);
		
		// bad object type
		revs = m_db.getObjectRevs(objType2Id, ATTR1, attrMap.get(ATTR1), false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());
		
		// 2 object revisions
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR2, attrMap.get(ATTR2), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR3, attrMap.get(ATTR3), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR4, attrMap.get(ATTR4), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR5, attrMap.get(ATTR5), false);
		assertEquals(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR6, attrMap.get(ATTR6), false);
		assertEquals(expectRevs, revs);

		/*
		 * two objects 
		 */
		// object 2 has 1 revision
		int obj2Rev1 = m_db.createObject(obj2Id, objType1Id, attrMap, false);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR2, attrMap.get(ATTR2), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR3, attrMap.get(ATTR3), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR4, attrMap.get(ATTR4), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR5, attrMap.get(ATTR5), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR6, attrMap.get(ATTR6), false);
		assertRevListMatch(expectRevs, revs);
		
		// object 2 has 2 revisions
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev2));
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR2, attrMap.get(ATTR2), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR3, attrMap.get(ATTR3), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR4, attrMap.get(ATTR4), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR5, attrMap.get(ATTR5), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR6, attrMap.get(ATTR6), false);
		assertRevListMatch(expectRevs, revs);
		
		
		m_db.setObjectValue(obj2Id, obj2Rev2, ATTR1, NEW_ATTR_VALUE);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR2, attrMap.get(ATTR2), false);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev1));
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev2));
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR3, attrMap.get(ATTR3), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR4, attrMap.get(ATTR4), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR5, attrMap.get(ATTR5), false);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR6, attrMap.get(ATTR6), false);
		assertRevListMatch(expectRevs, revs);
		
		// retrieve only last revisions that match
		revs = m_db.getObjectRevs(objType1Id, ATTR1, attrMap.get(ATTR1), true);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev1));
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR2, attrMap.get(ATTR2), true);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(new Revision(obj1Id, objType1Id, obj1Rev2));
		expectRevs.add(new Revision(obj2Id, objType1Id, obj2Rev2));
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR3, attrMap.get(ATTR3), true);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR4, attrMap.get(ATTR4), true);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR5, attrMap.get(ATTR5), true);
		assertRevListMatch(expectRevs, revs);
		
		revs = m_db.getObjectRevs(objType1Id, ATTR6, attrMap.get(ATTR6), true);
		assertRevListMatch(expectRevs, revs);
		
		// unexisting attributes
		revs = m_db.getObjectRevs(objType1Id, ATTR7, UUID.randomUUID(), false);
		assertNotNull(revs);
		assertTrue(revs.isEmpty());

		// invalid type of attribute
		try {
			m_db.getObjectRevs(objType1Id, ATTR1, UUID.randomUUID(), false);
			fail();
		} catch (ModelVersionDBException e) {
			// test passed
		}
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetObjectState() throws ModelVersionDBException {
		// null object id
		try {
			m_db.getObjectState(null, 1);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// invalid revision id
		try {
			m_db.createObject(obj1Id, objType1Id, null, false);
			m_db.getObjectState(obj1Id, -120);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
			m_db.deleteObject(obj1Id);
		}

		// Object does not exist
		try {
			m_db.getObjectState(obj1Id, 1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Object with empty state
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);

		// revision does not exist
		try {
			m_db.getObjectState(obj1Id, obj1Rev1 + 1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		Map<String, Object> stateMap = m_db.getObjectState(obj1Id, obj1Rev1);

		Map<String, Object> expectMap = new HashMap<String, Object>();
		assertEquals(expectMap, stateMap);
		
		// ALL and ANY forbidden
		try {
			m_db.getObjectState(obj1Id, ModelVersionDBService.ANY);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		try {
			m_db.getObjectState(obj1Id, ModelVersionDBService.ALL);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Object with one attribute
		Map<String, Object> obj2StateMap = new HashMap<String, Object>();
		obj2StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, obj2StateMap, true);

		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		expectMap = obj2StateMap;
		assertEquals(expectMap, stateMap);
		
		stateMap = m_db.getObjectState(obj2Id, ModelVersionDBService.LAST);
		expectMap = obj2StateMap;
		assertEquals(expectMap, stateMap);

		// Object with two attributes
		Map<String, Object> obj3StateMap = new HashMap<String, Object>();
		obj3StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		obj3StateMap.put(ATTR2, new Integer(34));
		obj3StateMap.put(ATTR3, notExistObjId);
		int obj3Rev1 = m_db.createObject(obj3Id, objType2Id, obj3StateMap,
				false);

		stateMap = m_db.getObjectState(obj3Id, obj3Rev1);

		expectMap = obj3StateMap;
		assertEquals(expectMap, stateMap);

		// update values
		obj3StateMap = new HashMap<String, Object>();
		obj3StateMap.put(ATTR2, new Integer(1234567));
		int obj3Rev2 = m_db.createNewObjectRevision(obj3Id, obj3Rev1);
		m_db.setObjectState(obj3Id, obj3Rev2, obj3StateMap);

		stateMap = m_db.getObjectState(obj3Id, obj3Rev1);
		
		expectMap = new HashMap<String, Object>();
		expectMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		expectMap.put(ATTR2, new Integer(34));
		expectMap.put(ATTR3, notExistObjId);
		assertEquals(expectMap, stateMap);
		
		stateMap = m_db.getObjectState(obj3Id, obj3Rev2);

		expectMap = new HashMap<String, Object>();
		expectMap.put(ATTR1, "first_attribute_value");
		expectMap.put(ATTR2, new Integer(1234567));
		expectMap.put(ATTR3, notExistObjId);
		assertEquals(expectMap, stateMap);
		
		stateMap = m_db.getObjectState(obj3Id, ModelVersionDBService.LAST);
		assertEquals(expectMap, stateMap);

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
			m_db.deleteObject(obj3Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testSetObjectState() throws ModelVersionDBException, TransactionException {
		Map<String, Object> obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);

		// Null object id
		try {
			m_db.setObjectState(null, 1, obj1StateMap);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Object does not exist
		try {
			m_db.setObjectState(notExistObjId, 1, obj1StateMap);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Empty State
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);

		obj1StateMap = new HashMap<String, Object>();
		try {
			m_db.setObjectState(obj1Id, obj1Rev1, obj1StateMap);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Null State
		try {
			m_db.setObjectState(obj1Id, obj1Rev1, null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// revision does not exist
		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		try {
			m_db.setObjectState(obj1Id, obj1Rev1 + 1, obj1StateMap);
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// one attribute value is
		// compatible with previous attribute type
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, FIRST_ATTRIBUTE_VALUE);

		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, NEW_ATTR_VALUE);
		m_db.setObjectState(obj1Id, obj1Rev1, obj1StateMap);

		Map<String, Object> stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertEquals(obj1StateMap, stateMap);

		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);

		try {
			m_db.setObjectState(obj1Id, ModelVersionDBService.ANY,
							obj1StateMap);
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		m_db.setObjectState(obj1Id, ModelVersionDBService.ALL, obj1StateMap);
		stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertEquals(obj1StateMap, stateMap);

		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, NEW_ATTR_VALUE);

		m_db.setObjectState(obj1Id, ModelVersionDBService.LAST, obj1StateMap);
		stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertEquals(obj1StateMap, stateMap);

		// attribute value is incompatible with previous attribute type
		// but it is migratable
		// try to set a very long string value
		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, NEW_ATTR_VALUE);
		obj1StateMap.put(ATTR2, null);
		obj1StateMap.put(ATTR3, null);
		obj1StateMap.put(ATTR4, null);
		obj1StateMap.put(ATTR5, null);
		obj1StateMap.put(ATTR6, null);
		obj1StateMap.put(ATTR7, null);
		m_db.setObjectState(obj1Id, obj1Rev1, obj1StateMap);

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < 10000; i++)
			sb.append("C");
		String veryLongStr = sb.toString();

		Map<String, Object> newStateMap = new HashMap<String, Object>();
		newStateMap.put(ATTR1, veryLongStr);
		newStateMap.put(ATTR2, NEW_ATTR_VALUE);
		newStateMap.put(ATTR3, new Integer(1234));
		newStateMap.put(ATTR4, new Long(1234567890L));
		newStateMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		newStateMap.put(ATTR6, new Ser("ser1"));
		newStateMap.put(ATTR7, new Boolean(false));
		m_db.setObjectState(obj1Id, obj1Rev1, newStateMap);

		// check state
		stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertEquals(newStateMap, stateMap);

		// reset to null
		newStateMap = obj1StateMap;
		m_db.setObjectState(obj1Id, obj1Rev1, newStateMap);

		// attribute value is incompatible with previous attribute type
		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, new Integer(1234));

		try {
			m_db.setObjectState(obj1Id, obj1Rev1, obj1StateMap);

			fail();
		} catch (ModelVersionDBException e) {
			if (!e.getMessage().startsWith("Found persist type "))
				fail();
		}

		// set a new attribute
		newStateMap.put(ATTR1, NEW_ATTR_VALUE);
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, newStateMap.get(ATTR1));

		newStateMap.put(ATTR2, "yet another value");
		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR2, newStateMap.get(ATTR2));

		m_db.setObjectState(obj1Id, obj1Rev1, obj1StateMap);
		stateMap = m_db.getObjectState(obj1Id, obj1Rev1);

		obj1StateMap.put(ATTR1, NEW_ATTR_VALUE);
		assertEquals(newStateMap, stateMap);

		// Check all attribute types
		Map<String, Object> obj2StateMap = new HashMap<String, Object>();
		obj2StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		obj2StateMap.put(ATTR2, null);
		obj2StateMap.put(ATTR3, new Integer(1234));
		obj2StateMap.put(ATTR4, new Long(1234567890L));
		obj2StateMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		obj2StateMap.put(ATTR6, new Ser("ser1"));
		int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, obj2StateMap, true);

		Ser newSer = new Ser("serial");
		java.util.Date newDate = new java.util.Date(System.currentTimeMillis());
		obj2StateMap.put(ATTR1, NEW_ATTR_VALUE);
		obj2StateMap.put(ATTR2, newSer);
		obj2StateMap.put(ATTR3, new Integer(5555));
		obj2StateMap.put(ATTR4, new Long(5555L));
		obj2StateMap.put(ATTR5, newDate);
		obj2StateMap.put(ATTR6, new Ser("ser2"));
		m_db.setObjectState(obj2Id, obj2Rev1, obj2StateMap);

		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		assertEquals(obj2StateMap, stateMap);
		
		// multiple object revisions 
		// with some shared (not version specific) attributes
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR2, false);
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR4, false);
		
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		Map<String, Object> obj2StateMap2 = new HashMap<String, Object>();
		obj2StateMap2.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		obj2StateMap2.put(ATTR2, new Ser("ser3"));
		obj2StateMap2.put(ATTR4, new Long(2345678901L));
		obj2StateMap2.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		obj2StateMap2.put(ATTR6, new Ser("ser4"));
		m_db.setObjectState(obj2Id, obj2Rev2, obj2StateMap2);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		Map<String, Object> expectObj2StateMap1 = new HashMap<String, Object>();
		expectObj2StateMap1.put(ATTR1, obj2StateMap.get(ATTR1));
		expectObj2StateMap1.put(ATTR2, obj2StateMap2.get(ATTR2));
		expectObj2StateMap1.put(ATTR3, obj2StateMap.get(ATTR3));
		expectObj2StateMap1.put(ATTR4, obj2StateMap2.get(ATTR4));
		expectObj2StateMap1.put(ATTR5, obj2StateMap.get(ATTR5));
		expectObj2StateMap1.put(ATTR6, obj2StateMap.get(ATTR6));
		assertEquals(expectObj2StateMap1, stateMap);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev2);
		Map<String, Object> expectObj2StateMap2 = new HashMap<String, Object>();
		expectObj2StateMap2.put(ATTR1, obj2StateMap2.get(ATTR1));
		expectObj2StateMap2.put(ATTR2, obj2StateMap2.get(ATTR2));
		expectObj2StateMap2.put(ATTR3, obj2StateMap.get(ATTR3));
		expectObj2StateMap2.put(ATTR4, obj2StateMap2.get(ATTR4));
		expectObj2StateMap2.put(ATTR5, obj2StateMap2.get(ATTR5));
		expectObj2StateMap2.put(ATTR6, obj2StateMap2.get(ATTR6));
		assertEquals(expectObj2StateMap2, stateMap);
		
		// set LAST revision
		Map<String, Object> newObj2StateMap2 = new HashMap<String, Object>();
		newObj2StateMap2.put(ATTR1, NEW_ATTR_VALUE);
		newObj2StateMap2.put(ATTR2, new Ser("ser5"));
		m_db.setObjectState(obj2Id, ModelVersionDBService.LAST, newObj2StateMap2);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		Map<String, Object> expectObj2StateMap1_2 = new HashMap<String, Object>();
		expectObj2StateMap1_2.put(ATTR1, expectObj2StateMap1.get(ATTR1));
		expectObj2StateMap1_2.put(ATTR2, newObj2StateMap2.get(ATTR2));
		expectObj2StateMap1_2.put(ATTR3, expectObj2StateMap1.get(ATTR3));
		expectObj2StateMap1_2.put(ATTR4, expectObj2StateMap1.get(ATTR4));
		expectObj2StateMap1_2.put(ATTR5, expectObj2StateMap1.get(ATTR5));
		expectObj2StateMap1_2.put(ATTR6, expectObj2StateMap1.get(ATTR6));
		assertEquals(expectObj2StateMap1_2, stateMap);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev2);
		Map<String, Object> expectObj2StateMap2_2 = new HashMap<String, Object>();
		expectObj2StateMap2_2.put(ATTR1, newObj2StateMap2.get(ATTR1));
		expectObj2StateMap2_2.put(ATTR2, newObj2StateMap2.get(ATTR2));
		expectObj2StateMap2_2.put(ATTR3, expectObj2StateMap2.get(ATTR3));
		expectObj2StateMap2_2.put(ATTR4, expectObj2StateMap2.get(ATTR4));
		expectObj2StateMap2_2.put(ATTR5, expectObj2StateMap2.get(ATTR5));
		expectObj2StateMap2_2.put(ATTR6, expectObj2StateMap2.get(ATTR6));
		assertEquals(expectObj2StateMap2_2, stateMap);
		
		// set ALL revision
		Map<String, Object> newObj2StateMap = new HashMap<String, Object>();
		newObj2StateMap.put(ATTR3, new Integer(33333));
		newObj2StateMap.put(ATTR4, new Long(3456789012L));
		m_db.setObjectState(obj2Id, ModelVersionDBService.ALL, newObj2StateMap);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		Map<String, Object> expectObj2StateMap1_3 = new HashMap<String, Object>();
		expectObj2StateMap1_3.put(ATTR1, expectObj2StateMap1_2.get(ATTR1));
		expectObj2StateMap1_3.put(ATTR2, expectObj2StateMap1_2.get(ATTR2));
		expectObj2StateMap1_3.put(ATTR3, newObj2StateMap.get(ATTR3));
		expectObj2StateMap1_3.put(ATTR4, newObj2StateMap.get(ATTR4));
		expectObj2StateMap1_3.put(ATTR5, expectObj2StateMap1_2.get(ATTR5));
		expectObj2StateMap1_3.put(ATTR6, expectObj2StateMap1_2.get(ATTR6));
		assertEquals(expectObj2StateMap1_3, stateMap);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev2);
		Map<String, Object> expectObj2StateMap2_3 = new HashMap<String, Object>();
		expectObj2StateMap2_3.put(ATTR1, expectObj2StateMap2_2.get(ATTR1));
		expectObj2StateMap2_3.put(ATTR2, expectObj2StateMap2_2.get(ATTR2));
		expectObj2StateMap2_3.put(ATTR3, newObj2StateMap.get(ATTR3));
		expectObj2StateMap2_3.put(ATTR4, newObj2StateMap.get(ATTR4));
		expectObj2StateMap2_3.put(ATTR5, expectObj2StateMap2_2.get(ATTR5));
		expectObj2StateMap2_3.put(ATTR6, expectObj2StateMap2_2.get(ATTR6));
		assertEquals(expectObj2StateMap2_3, stateMap);

		// check transaction support
		// rollback transaction
		m_db.beginTransaction();
		Map<String, Object> newObj2StateMap3 = new HashMap<String, Object>();
		newObj2StateMap3.put(ATTR3, new Integer(444));
		newObj2StateMap3.put(ATTR4, new Long(4567890123L));
		m_db.setObjectState(obj2Id, obj2Rev2, newObj2StateMap3);
		m_db.rollbackTransaction();
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		assertEquals(expectObj2StateMap1_3, stateMap);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev2);
		assertEquals(expectObj2StateMap2_3, stateMap);
		
		// commit transaction
		m_db.beginTransaction();
		m_db.setObjectState(obj2Id, obj2Rev2, newObj2StateMap3);
		m_db.commitTransaction();
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		Map<String, Object> expectObj2StateMap1_4 = new HashMap<String, Object>();
		expectObj2StateMap1_4.put(ATTR1, expectObj2StateMap1_3.get(ATTR1));
		expectObj2StateMap1_4.put(ATTR2, expectObj2StateMap1_3.get(ATTR2));
		expectObj2StateMap1_4.put(ATTR3, expectObj2StateMap1_3.get(ATTR3));
		expectObj2StateMap1_4.put(ATTR4, newObj2StateMap3.get(ATTR4));
		expectObj2StateMap1_4.put(ATTR5, expectObj2StateMap1_3.get(ATTR5));
		expectObj2StateMap1_4.put(ATTR6, expectObj2StateMap1_3.get(ATTR6));
		assertEquals(expectObj2StateMap1_4, stateMap);
		
		stateMap = m_db.getObjectState(obj2Id, obj2Rev2);
		Map<String, Object> expectObj2StateMap2_4 = new HashMap<String, Object>();
		expectObj2StateMap2_4.put(ATTR1, expectObj2StateMap2_3.get(ATTR1));
		expectObj2StateMap2_4.put(ATTR2, expectObj2StateMap2_3.get(ATTR2));
		expectObj2StateMap2_4.put(ATTR3, newObj2StateMap3.get(ATTR3));
		expectObj2StateMap2_4.put(ATTR4, newObj2StateMap3.get(ATTR4));
		expectObj2StateMap2_4.put(ATTR5, expectObj2StateMap2_3.get(ATTR5));
		expectObj2StateMap2_4.put(ATTR6, expectObj2StateMap2_3.get(ATTR6));
		assertEquals(expectObj2StateMap2_4, stateMap);
		
		// cleaning code
		try {
			m_db.setObjectAttVersionSpecific(objType2Id, ATTR2, true);
			m_db.setObjectAttVersionSpecific(objType2Id, ATTR4, true);
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetObjectValue() throws ModelVersionDBException {
		// null object id
		try {
			m_db.getObjectValue(null, 1, ATTR1);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Object does not exist
		try {
			m_db.getObjectValue(notExistObjId, 1, ATTR1);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		Map<String, Object> obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, obj1StateMap,
				false);

		// revision does not exist
		try {
			m_db.getObjectValue(obj1Id, obj1Rev1 + 1, ATTR1);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// null attribute name
		try {
			m_db.getObjectValue(obj1Id, obj1Rev1, null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// attribute does not exist
		try {
			m_db.getObjectValue(obj1Id, obj1Rev1, ATTR7);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Check all attribute types
		Map<String, Object> obj2StateMap = new HashMap<String, Object>();
		obj2StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		obj2StateMap.put(ATTR2, null);
		obj2StateMap.put(ATTR3, new Integer(1234));
		obj2StateMap.put(ATTR4, new Long(1234567890L));
		obj2StateMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		obj2StateMap.put(ATTR6, new Ser("ser1"));
		obj2StateMap.put(ATTR7, UUID.randomUUID());
		int obj2Rev1 = m_db
				.createObject(obj2Id, objType2Id, obj2StateMap, true);

		Object value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR1);
		assertEquals(obj2StateMap.get(ATTR1), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR2);
		assertEquals(obj2StateMap.get(ATTR2), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR3);
		assertEquals(obj2StateMap.get(ATTR3), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR4);
		assertEquals(obj2StateMap.get(ATTR4), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR5);
		assertEquals(obj2StateMap.get(ATTR5), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR6);
		assertEquals(obj2StateMap.get(ATTR6), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR7);
		assertEquals(obj2StateMap.get(ATTR7), value);
		
		// multiple revisions
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		Map<String, Object> obj2StateMap2 = new HashMap<String, Object>();
		obj2StateMap2.put(ATTR1, NEW_ATTR_VALUE);
		obj2StateMap2.put(ATTR2, new Ser("newSer"));
		m_db.setObjectState(obj2Id, obj2Rev2, obj2StateMap2);
		
		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR1);
		assertEquals(obj2StateMap2.get(ATTR1), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR2);
		assertEquals(obj2StateMap2.get(ATTR2), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR3);
		assertEquals(obj2StateMap.get(ATTR3), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR4);
		assertEquals(obj2StateMap.get(ATTR4), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR5);
		assertEquals(obj2StateMap.get(ATTR5), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR6);
		assertEquals(obj2StateMap.get(ATTR6), value);

		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR7);
		assertEquals(obj2StateMap.get(ATTR7), value);
		
		// using LAST rev number
		value = m_db.getObjectValue(obj2Id, ModelVersionDBService.LAST, ATTR1);
		assertEquals(obj2StateMap2.get(ATTR1), value);
		
		// using ALL rev number
		try {
			m_db.getObjectValue(obj1Id, ModelVersionDBService.ALL, ATTR1);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// using ANY rev number
		try {
			m_db.getObjectValue(obj1Id, ModelVersionDBService.ANY, ATTR1);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testSetObjectValue() throws ModelVersionDBException, TransactionException {
		// Object does not exist
		try {
			m_db.setObjectValue(notExistObjId, 1, ATTR1, NEW_ATTR_VALUE);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// attribute value is compatible with previous attribute
		Map<String, Object> obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, obj1StateMap,
				false);

		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, NEW_ATTR_VALUE);
		Object value = m_db.getObjectValue(obj1Id, obj1Rev1, ATTR1);
		assertEquals(NEW_ATTR_VALUE, value);

		// attribute value is incompatible
		// with previous attribute type but it is migratable
		// try to set a very long string value
		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, NEW_ATTR_VALUE);
		obj1StateMap.put(ATTR2, null);
		obj1StateMap.put(ATTR3, null);
		obj1StateMap.put(ATTR4, null);
		obj1StateMap.put(ATTR5, null);
		obj1StateMap.put(ATTR6, null);
		obj1StateMap.put(ATTR7, null);
		m_db.setObjectState(obj1Id, obj1Rev1, obj1StateMap);

		// new state map
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < 10000; i++)
			sb.append("C");
		String veryLongStr = sb.toString();

		Map<String, Object> expectedMap = new HashMap<String, Object>();
		expectedMap.put(ATTR1, veryLongStr);
		expectedMap.put(ATTR2, NEW_ATTR_VALUE);
		expectedMap.put(ATTR3, new Integer(1234));
		expectedMap.put(ATTR4, new Long(1234567890L));
		expectedMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		expectedMap.put(ATTR6, new Ser("ser1"));
		expectedMap.put(ATTR7, new Boolean(false));

		// string size migration
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, expectedMap.get(ATTR1));

		// null to string migration
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR2, expectedMap.get(ATTR2));

		// null to Integer migration
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR3, expectedMap.get(ATTR3));

		// null to Long migration
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR4, expectedMap.get(ATTR4));

		// null to Date migration
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR5, expectedMap.get(ATTR5));

		// null to Serializable migration
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR6, expectedMap.get(ATTR6));

		// null to Boolean migration
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR7, expectedMap.get(ATTR7));

		// check state Map<String, Object>
		Map<String, Object> stateMap = m_db.getObjectState(obj1Id, obj1Rev1);

		assertEquals(expectedMap, stateMap);

		// attribute value is incompatible with previous attribute type
		try {
			m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, new Integer(1234));

			fail();
		} catch (ModelVersionDBException e) {
			if (!e.getMessage().startsWith("Found persist type "))
				fail();
		}

		// Check all attribute types
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR2, false);
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR4, false);
		
		Map<String, Object> obj2StateMap = new HashMap<String, Object>();
		obj2StateMap.put(ATTR1, "first_attribute_value");
		obj2StateMap.put(ATTR2, null);
		obj2StateMap.put(ATTR3, new Integer(1234));
		obj2StateMap.put(ATTR4, new Long(1234567890L));
		obj2StateMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		obj2StateMap.put(ATTR6, new Ser("ser1"));
		obj2StateMap.put(ATTR7, UUID.randomUUID());
		int obj2Rev1 = m_db
				.createObject(obj2Id, objType2Id, obj2StateMap, true);

		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR1, NEW_ATTR_VALUE);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR1);
		assertEquals(NEW_ATTR_VALUE, value);

		Ser newSer2 = new Ser("serial");
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR2, newSer2);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR2);
		assertEquals(newSer2, value);

		Integer newInt = new Integer(5555);
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR3, newInt);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR3);
		assertEquals(newInt, value);

		Long newLong = new Long(5555L);
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR4, newLong);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR4);
		assertEquals(newLong, value);

		java.util.Date newDate = new java.sql.Date(System.currentTimeMillis());
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR5, newDate);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR5);
		assertEquals(newDate, value);

		Ser newSer6 = new Ser("ser2");
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR6, newSer6);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR6);
		assertEquals(newSer6, value);

		UUID newId = UUID.randomUUID();
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR7, newId);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR7);
		assertEquals(newId, value);
		
		// multiple revision
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		
		m_db.setObjectValue(obj2Id, obj2Rev2, ATTR1, FIRST_ATTRIBUTE_VALUE);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR1);
		assertEquals(NEW_ATTR_VALUE, value);
		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR1);
		assertEquals(FIRST_ATTRIBUTE_VALUE, value);
		
		// not version specific attributes
		Ser newSer2_2 = new Ser("AnotherSer");
		m_db.setObjectValue(obj2Id, obj2Rev2, ATTR2, newSer2_2);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR2);
		assertEquals(newSer2_2, value);
		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR2);
		assertEquals(newSer2_2, value);
		
		Long newLong2_2 = new Long(12345);
		m_db.setObjectValue(obj2Id, obj2Rev2, ATTR4, newLong2_2);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR4);
		assertEquals(newLong2_2, value);
		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR4);
		assertEquals(newLong2_2, value);
		
		// LAST revision
		m_db.setObjectValue(obj2Id, ModelVersionDBService.LAST, ATTR1, NEW_ATTR_VALUE);
		value = m_db.getObjectValue(obj2Id, ModelVersionDBService.LAST, ATTR1);
		assertEquals(NEW_ATTR_VALUE, value);
		
		// ANY revision
		try {
			m_db.setObjectValue(obj2Id, ModelVersionDBService.ANY, ATTR1, FIRST_ATTRIBUTE_VALUE);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// ALL revision
		m_db.setObjectValue(obj2Id, ModelVersionDBService.ALL, ATTR1, NEW_NEW_ATTR_VALUE);
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR1);
		assertEquals(NEW_NEW_ATTR_VALUE, value);
		value = m_db.getObjectValue(obj2Id, obj2Rev2, ATTR1);
		assertEquals(NEW_NEW_ATTR_VALUE, value);

		// revision does not exist
		try {
			m_db.setObjectValue(obj2Id, obj2Rev2 + 1, ATTR1, NEW_ATTR_VALUE);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// Null object id
		try {
			m_db.setObjectValue(null, obj2Rev1, ATTR1, NEW_ATTR_VALUE);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Null attribute name
		try {
			m_db.setObjectValue(obj2Id, obj2Rev1, null, NEW_ATTR_VALUE);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// check transaction support
		// rollback transaction
		m_db.beginTransaction();
		Integer newIntToSet = new Integer(66666);
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR3, newIntToSet);
		m_db.rollbackTransaction();
		
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR3);
		assertEquals(newInt, value);
		
		// commit transaction
		m_db.beginTransaction();
		m_db.setObjectValue(obj2Id, obj2Rev1, ATTR3, newIntToSet);
		m_db.commitTransaction();
		
		value = m_db.getObjectValue(obj2Id, obj2Rev1, ATTR3);
		assertEquals(newIntToSet, value);

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetObjectType() throws ModelVersionDBException {
		// Null object id
		try {
			m_db.getObjectType(null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Object does not exist
		try {
			m_db.getObjectType(notExistObjId);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Object exists
		m_db.createObject(obj1Id, objType1Id, null, false);

		UUID typeId = m_db.getObjectType(obj1Id);
		assertEquals(objType1Id, typeId);

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testCreateObject() throws ModelVersionDBException, TransactionException {
		// Null object id
		Map<String, Object> obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, NEW_ATTR_VALUE);

		try {
			m_db.createObject(null, objType1Id, obj1StateMap, false);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Null type id
		try {
			m_db.createObject(obj1Id, null, obj1StateMap, false);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Null state
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);

		Map<String, Object> stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertTrue(stateMap.isEmpty());
		assertEquals(objType1Id, m_db.getObjectType(obj1Id));
		assertFalse(m_db.isType(obj1Id));

		// Empty State
		m_db.deleteObject(obj1Id);

		obj1Rev1 = m_db.createObject(obj1Id, objType1Id,
				new HashMap<String, Object>(), false);

		stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertTrue(stateMap.isEmpty());
		assertEquals(objType1Id, m_db.getObjectType(obj1Id));
		assertFalse(m_db.isType(obj1Id));

		// Existing link
		Revision rev = m_db.addLink(linkType1Id, obj1Id, obj1Rev1, obj1Id,
				obj1Rev1, null);
		m_db.createObject(rev.getId(), objType1Id, null, false);

		stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertTrue(stateMap.isEmpty());
		assertEquals(objType1Id, m_db.getObjectType(obj1Id));
		assertFalse(m_db.isType(obj1Id));

		// one attribute
		m_db.deleteObject(obj1Id);

		obj1StateMap = new HashMap<String, Object>();
		obj1StateMap.put(ATTR1, NEW_ATTR_VALUE);
		obj1Rev1 = m_db.createObject(obj1Id, objType1Id, obj1StateMap, true);

		stateMap = m_db.getObjectState(obj1Id, obj1Rev1);
		assertEquals(obj1StateMap, stateMap);
		assertEquals(objType1Id, m_db.getObjectType(obj1Id));
		assertTrue(m_db.isType(obj1Id));

		// Check all attribute types
		Map<String, Object> obj2StateMap = new HashMap<String, Object>();
		obj2StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		obj2StateMap.put(ATTR2, null);
		obj2StateMap.put(ATTR3, new Integer(1234));
		obj2StateMap.put(ATTR4, new Long(1234567890L));
		obj2StateMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		obj2StateMap.put(ATTR6, new Ser("ser1"));
		obj2StateMap.put(ATTR7, new Boolean(false));
		obj2StateMap.put(ATTR8, new Boolean(true));
		int obj2Rev1 = m_db
				.createObject(obj2Id, objType2Id, obj2StateMap, true);

		stateMap = m_db.getObjectState(obj2Id, obj2Rev1);
		assertEquals(obj2StateMap, stateMap);
		assertEquals(objType2Id, m_db.getObjectType(obj2Id));
		assertTrue(m_db.isType(obj2Id));
		
		// check transaction support
		// rollback transaction
		m_db.beginTransaction();
		m_db.createObject(obj3Id, objType2Id, null, false);
		m_db.rollbackTransaction();
		
		assertFalse(m_db.objExists(obj3Id));
		
		// commit transaction
		m_db.beginTransaction();
		m_db.createObject(obj3Id, objType2Id, null, false);
		m_db.commitTransaction();
		
		assertTrue(m_db.objExists(obj3Id));

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
			m_db.deleteObject(obj3Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testSetObjectAttVersionSpecific() throws ModelVersionDBException, TransactionException {
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		String valRev1 = FIRST_ATTRIBUTE_VALUE;
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, valRev1);
		
		// check default value
		assertTrue(m_db.isObjectAttVersionSpecific(objType1Id, ATTR1));
		
		// null type id
		try {
			m_db.setObjectAttVersionSpecific(null, ATTR1, false);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null attribute name 
		try {
			m_db.setObjectAttVersionSpecific(objType1Id, null, false);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// existing type and existing attribute
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR1, false);
		assertFalse(m_db.isObjectAttVersionSpecific(objType1Id, ATTR1));
		
		// existing type and unexisting attribute
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR2, false);
		assertFalse(m_db.isObjectAttVersionSpecific(objType1Id, ATTR2));
		
		// unexisting type 
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR1, false);
		assertFalse(m_db.isObjectAttVersionSpecific(objType1Id, ATTR2));
		
		// set true value
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR1, true);
		assertTrue(m_db.isObjectAttVersionSpecific(objType1Id, ATTR1));
		
		// change to true to false
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR1, true);
		
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		String valRev2 = NEW_ATTR_VALUE;
		m_db.setObjectValue(obj1Id, obj1Rev2, ATTR1, valRev2);
		
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR1, false);
		assertFalse(m_db.isObjectAttVersionSpecific(objType1Id, ATTR1));
		
		assertEquals(valRev2, m_db.getObjectValue(obj1Id, obj1Rev1, ATTR1));
		assertEquals(valRev2, m_db.getObjectValue(obj1Id, obj1Rev2, ATTR1));
		
		/*
		 * check transaction support
		 */
		
		if (getBaseType().equals(ModelVersionDBService.ORACLE_TYPE) ||
			getBaseType().equals(ModelVersionDBService.MYSQL_TYPE) ||
			getBaseType().equals(ModelVersionDBService.HSQL_IN_MEMORY_TYPE) ||
			getBaseType().equals(ModelVersionDBService.HSQL_TYPE)) {
			// HSQL and MySQL cannot rollback alter table sql commands.
			// So, rollback action on new attributes does not work
			m_db.setObjectAttVersionSpecific(objType2Id, ATTR3, false);
		}
		
		// rollback transaction of unexisting attribute
		m_db.beginTransaction();
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR3, true);
		m_db.rollbackTransaction();
		
		assertFalse(m_db.isObjectAttVersionSpecific(objType2Id, ATTR3));
		
		// commit transaction
		m_db.beginTransaction();
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR3, false);
		m_db.commitTransaction();
		
		assertFalse(m_db.isObjectAttVersionSpecific(objType2Id, ATTR3));

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testIsObjectAttVersionSpecific() throws ModelVersionDBException {
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		m_db.setObjectValue(obj1Id, obj1Rev1, ATTR1, FIRST_ATTRIBUTE_VALUE);
		
		// check default value
		assertTrue(m_db.isObjectAttVersionSpecific(objType1Id, ATTR1));
		
		// null type id
		try {
			m_db.isObjectAttVersionSpecific(null, ATTR1);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null attribute name 
		try {
			m_db.isObjectAttVersionSpecific(objType1Id, null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// existing type and existing attribute
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR1, false);
		assertFalse(m_db.isObjectAttVersionSpecific(objType1Id, ATTR1));
		
		// existing type and unexisting attribute
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR2, false);
		assertFalse(m_db.isObjectAttVersionSpecific(objType1Id, ATTR2));
		
		// unexisting type 
		m_db.setObjectAttVersionSpecific(objType2Id, ATTR1, false);
		assertFalse(m_db.isObjectAttVersionSpecific(objType1Id, ATTR2));
		
		// set true value
		m_db.setObjectAttVersionSpecific(objType1Id, ATTR1, true);
		assertTrue(m_db.isObjectAttVersionSpecific(objType1Id, ATTR1));
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testCreateNewObjectRevision() throws ModelVersionDBException, TransactionException {
		Map<String, Object> attrMap = new HashMap<String, Object>();
		attrMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		attrMap.put(ATTR2, null);
		attrMap.put(ATTR3, new Integer(1234));
		attrMap.put(ATTR4, new Long(1234567890L));
		attrMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
		attrMap.put(ATTR6, new Ser("ser1"));
		
		/*
		 * One object 
		 */
		
		// One object revision
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, attrMap, false);

		// Null object id
		try {
			m_db.createNewObjectRevision(null, obj1Rev1);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// object id does not exist
		try {
			m_db.createNewObjectRevision(notExistObjId, obj1Rev1);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// object revision does not exist
		try {
			m_db.createNewObjectRevision(obj1Id, obj1Rev1 + 1);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// use ALL
		try {
			m_db.createNewObjectRevision(obj1Id, ModelVersionDBService.ALL);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// use ANY
		try {
			m_db.createNewObjectRevision(obj1Id, ModelVersionDBService.ANY);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		/*
		 * Two objects
		 */
		int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, null, true);
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		
		// create links for first revision
		Map<String, Object> linkStateMap1 = new HashMap<String, Object>();
		linkStateMap1.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		linkStateMap1.put(ATTR3, new Integer(1234));
		
		Map<String, Object> linkStateMap2 = new HashMap<String, Object>();
		linkStateMap2.put(ATTR1, NEW_ATTR_VALUE);
		linkStateMap2.put(ATTR3, new Integer(234));
		
		Map<String, Object> linkStateMap3 = new HashMap<String, Object>();
		linkStateMap3.put(ATTR1, NEW_NEW_ATTR_VALUE);
		linkStateMap3.put(ATTR3, new Integer(34));
		
		m_db.addLink(linkType1Id, obj1Id, obj1Rev1, obj2Id, obj2Rev1, null);
		m_db.addLink(linkType2Id, obj1Id, obj1Rev1, obj2Id, obj2Rev1, linkStateMap1);
		m_db.addLink(linkType2Id, obj1Id, obj1Rev1, obj2Id, obj2Rev2, linkStateMap2);
		m_db.addLink(linkType3Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1, linkStateMap3);
		
		m_db.setLinkSrcVersionSpecific(linkType3Id, false);
		
		// two revisions
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		assertEquals(attrMap, m_db.getObjectState(obj1Id, obj1Rev2));
		
		// check outgoing links
		checkLinkRevs(linkType1Id, obj1Id, obj1Rev2, 
				new CheckRevision(obj2Id, obj2Rev1, null));
		
		checkLinkRevs(linkType2Id, obj1Id, obj1Rev2, 
				new CheckRevision(obj2Id, obj2Rev1, linkStateMap1),
				new CheckRevision(obj2Id, obj2Rev2, linkStateMap2));
		
		checkLinkRevs(linkType3Id, obj1Id, obj1Rev2, 
				new CheckRevision(obj1Id, obj1Rev1, linkStateMap3));
		
		// use LAST
		int obj1Rev3 = m_db.createNewObjectRevision(obj1Id, ModelVersionDBService.LAST);
		
		// check outgoing links
		checkLinkRevs(linkType1Id, obj1Id, obj1Rev3, 
				new CheckRevision(obj2Id, obj2Rev1, null));
		
		checkLinkRevs(linkType2Id, obj1Id, obj1Rev3, 
				new CheckRevision(obj2Id, obj2Rev1, linkStateMap1),
				new CheckRevision(obj2Id, obj2Rev2, linkStateMap2));
		
		checkLinkRevs(linkType3Id, obj1Id, obj1Rev3, 
				new CheckRevision(obj1Id, obj1Rev1, linkStateMap3),
				new CheckRevision(obj1Id, obj1Rev2, linkStateMap3),
				new CheckRevision(obj1Id, obj1Rev3, linkStateMap3));
		
		checkLinkRevs(linkType3Id, obj1Id, obj1Rev2, 
				new CheckRevision(obj1Id, obj1Rev1, linkStateMap3),
				new CheckRevision(obj1Id, obj1Rev2, linkStateMap3),
				new CheckRevision(obj1Id, obj1Rev3, linkStateMap3));
		
		checkLinkRevs(linkType3Id, obj1Id, obj1Rev1, 
				new CheckRevision(obj1Id, obj1Rev1, linkStateMap3),
				new CheckRevision(obj1Id, obj1Rev2, linkStateMap3),
				new CheckRevision(obj1Id, obj1Rev3, linkStateMap3));
		
		// check transaction support
		// rollback transaction
		m_db.beginTransaction();
		int obj1Rev4 = m_db.createNewObjectRevision(obj1Id, ModelVersionDBService.LAST);
		m_db.rollbackTransaction();
		
		assertFalse(m_db.objExists(obj1Id, obj1Rev4));
		
		// commit transaction
		m_db.beginTransaction();
		obj1Rev4 = m_db.createNewObjectRevision(obj1Id, ModelVersionDBService.LAST);
		m_db.commitTransaction();
		
		assertTrue(m_db.objExists(obj1Id, obj1Rev4));

		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}

	public void testGetLastObjectRevNb() throws ModelVersionDBException {
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		
		// null object
		try {
			m_db.getLastObjectRevNb(null);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// unexisting object
		try {
			m_db.getLastObjectRevNb(notExistObjId);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// one revision
		assertEquals(obj1Rev1, m_db.getLastObjectRevNb(obj1Id));
		
		// two revisions
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		
		assertEquals(obj1Rev2, m_db.getLastObjectRevNb(obj1Id));
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetObjectRevNbs() throws ModelVersionDBException {
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		
		// null object
		try {
			m_db.getObjectRevNbs(null);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// unexisting object
		try {
			m_db.getObjectRevNbs(notExistObjId);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		} 
		
		// one revision
		int[] revs = m_db.getObjectRevNbs(obj1Id);
		assertContainsRevs(revs, obj1Rev1);
		
		// two revisions
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		
		revs = m_db.getObjectRevNbs(obj1Id);
		assertContainsRevs(revs, obj1Rev1, obj1Rev2);
		
		// cleaning code
		try {
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testDeleteObject() throws ModelVersionDBException, TransactionException {
		// null object id
		try {
			m_db.deleteObject(null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Not existing object and link
		try {
			m_db.deleteObject(notExistObjId);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		/*
		 * Existing object
		 */
		// one revision
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		m_db.deleteObject(obj1Id);
		
		assertFalse(m_db.objExists(obj1Id));
		assertFalse(m_db.objExists(obj1Id, obj1Rev1));
		
		// with incoming and outgoing links
		obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		int obj2Rev1 = m_db.createObject(obj2Id, objType1Id, null, false);
		Revision incomingLinkRev = m_db.addLink(linkType1Id, obj2Id, obj2Rev1, obj1Id, obj1Rev1, null);
		Revision outgoingLinkRev = m_db.addLink(linkType1Id, obj1Id, obj1Rev1, obj2Id, obj2Rev1, null);
		
		m_db.deleteObject(obj1Id);
		
		assertFalse(m_db.objExists(obj1Id));
		assertFalse(m_db.objExists(obj1Id, obj1Rev1));
		assertFalse(m_db.linkExists(incomingLinkRev.getId(), incomingLinkRev.getRev()));
		assertFalse(m_db.linkExists(outgoingLinkRev.getId(), outgoingLinkRev.getRev()));
		
		// two revisions
		obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		m_db.deleteObject(obj1Id);
		
		assertFalse(m_db.objExists(obj1Id));
		assertFalse(m_db.objExists(obj1Id, obj1Rev1));
		assertFalse(m_db.objExists(obj1Id, obj1Rev2));
		
		// check transaction support
		// rollback transaction
		obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		m_db.beginTransaction();
		m_db.deleteObject(obj1Id);
		m_db.rollbackTransaction();
		
		assertTrue(m_db.objExists(obj1Id, obj1Rev1));
		
		// commit transaction
		m_db.beginTransaction();
		m_db.deleteObject(obj1Id);
		m_db.commitTransaction();
		
		assertFalse(m_db.objExists(obj1Id, obj1Rev1));

		// cleaning code
		try {
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testLinkExistsWithID() throws ModelVersionDBException {
		
		// null link id
		try {
			m_db.linkExists(null);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// Not existing object and link
		assertFalse(m_db.linkExists(notExistLinkId));
		
		// Existing object
		int obj1Rev = m_db.createObject(obj1Id, objType1Id, null, false);
		assertFalse(m_db.linkExists(obj1Id));
		
		// Existing link 
		Revision linkRev = m_db.addLink(linkType1Id, obj1Id, obj1Rev, obj1Id, obj1Rev, null);
		assertTrue(m_db.linkExists(linkRev.getId()));
		
		// cleaning code
		try {
			m_db.deleteLink(linkRev.getId());
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testLinkExistsWithIdAndRev() throws ModelVersionDBException {
		
		// null link id
		try {
			m_db.linkExists(null, 1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// Not existing object and link
		assertFalse(m_db.linkExists(notExistLinkId, 1));
		
		try {
			m_db.linkExists(notExistLinkId, ModelVersionDBService.ALL);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		assertFalse(m_db.linkExists(notExistLinkId, ModelVersionDBService.ANY));
		
		assertFalse(m_db.linkExists(notExistLinkId, ModelVersionDBService.LAST));
		
		// Existing object
		int obj1Rev = m_db.createObject(obj1Id, objType1Id, null, false);
		assertFalse(m_db.linkExists(obj1Id, obj1Rev));
		
		// Existing link and object not exist
		Revision linkRev = m_db.addLink(linkType1Id, obj1Id, obj1Rev, obj1Id, obj1Rev, null);
		assertTrue(m_db.linkExists(linkRev.getId(), linkRev.getRev()));
		
		try {
			m_db.linkExists(obj1Id, ModelVersionDBService.ALL);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		assertTrue(m_db.linkExists(linkRev.getId(), ModelVersionDBService.ANY));
		
		assertTrue(m_db.linkExists(linkRev.getId(), ModelVersionDBService.LAST));
		
		// Revision does not exist
		assertFalse(m_db.linkExists(linkRev.getId(), linkRev.getRev() + 1));
		
		// cleaning code
		try {
			m_db.deleteLink(linkRev.getId());
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testLinkExistsWithTypeIdAndSrcRevAndDestRev() throws ModelVersionDBException {
		
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		
		// null link type id
		try {
			m_db.linkExists(null, obj1Id, obj1Rev1, obj1Id, obj1Rev1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null source id
		try {
			m_db.linkExists(linkType1Id, null, obj1Rev1, obj1Id, obj1Rev1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null destination id
		try {
			m_db.linkExists(linkType1Id, obj1Id, obj1Rev1, null, obj1Rev1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// Not existing link
		assertFalse(m_db.linkExists(linkType1Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1));
		
		try {
			m_db.linkExists(linkType1Id, obj1Id, ModelVersionDBService.ALL, obj1Id, obj1Rev1);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		try {
			m_db.linkExists(linkType1Id, obj1Id, obj1Rev1, obj1Id, ModelVersionDBService.ALL);
			
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		assertFalse(m_db.linkExists(notExistLinkId, ModelVersionDBService.ANY));
		
		assertFalse(m_db.linkExists(notExistLinkId, ModelVersionDBService.LAST));
		
		// Existing link 
		Revision linkRev = m_db.addLink(linkType1Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1, null);
		assertTrue(m_db.linkExists(linkType1Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1));
		
		assertTrue(m_db.linkExists(linkType1Id, obj1Id, obj1Rev1, obj1Id, ModelVersionDBService.ANY));
		
		assertTrue(m_db.linkExists(linkType1Id, obj1Id, ModelVersionDBService.LAST, obj1Id, obj1Rev1));
		
		// Revision does not exist
		assertFalse(m_db.linkExists(linkType1Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1 + 1));
		
		// cleaning code
		try {
			m_db.deleteLink(linkRev.getId());
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetLinkType() throws ModelVersionDBException {
		// null link Id
		try {
			m_db.getLinkType(null);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Link does not exist
		try {
			m_db.getLinkType(notExistLinkId);
			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// link exist
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		Revision linkRev = m_db.addLink(linkType1Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1, null);

		UUID typeId = m_db.getLinkType(linkRev.getId());
		assertEquals(linkType1Id, typeId);

		// cleaning code
		try {
			m_db.deleteLink(linkRev.getId());
			m_db.deleteObject(obj1Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetLinks() throws ModelVersionDBException { 
		// No link in base 
		Set<UUID> links = m_db.getLinks();
		  
		  // link ids order is not specified in general
		  Set<UUID> expectedSet = new HashSet<UUID>(); 
		  assertEquals(expectedSet, links); 
		  
		  // One link 
		  int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		  Revision link1Rev1 = m_db.addLink(linkType1Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1, null);
		  
		  links = m_db.getLinks();
		  
		  expectedSet = new HashSet<UUID>(); 
		  expectedSet.add(link1Rev1.getId());
		  assertEquals(links, links); 
		  
		  // Two links 
		  Revision link2Rev1 = m_db.addLink(linkType2Id, obj1Id, obj1Rev1, obj1Id, obj1Rev1, null);
		  
		  links = m_db.getLinks();
		  
		  expectedSet = new HashSet<UUID>(); 
		  expectedSet.add(link1Rev1.getId());
		  expectedSet.add(link2Rev1.getId());
		  assertEquals(links, expectedSet); 
		  
		  // cleaning code 
		  try {
			  m_db.deleteLink(link1Rev1.getId()); 
			  m_db.deleteLink(link2Rev1.getId()); 
			  m_db.deleteObject(obj1Id); }
		  catch (Exception e) { 
			  // ignore it 
		  } 
	}
	
	public void testGetLinksWithTypeId() throws ModelVersionDBException {
		// null type id
		try {
			m_db.getLinks(null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// No link in base try { List<UUID>
		List<UUID> links = m_db.getLinks(linkType1Id);

		List<UUID> expectList = new ArrayList<UUID>();
		assertEquals(expectList, links);

		// One link
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		Revision link1Rev1 = m_db.addLink(linkType1Id, obj1Id, obj1Rev1,
				obj1Id, obj1Rev1, null);

		links = m_db.getLinks(linkType1Id);

		expectList = new ArrayList<UUID>();
		expectList.add(link1Rev1.getId());
		assertEquals(expectList, links);

		// Two links
		int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, null, true);
		Revision link2Rev1 = m_db.addLink(linkType1Id, obj1Id, obj1Rev1,
				obj2Id, obj2Rev1, null);

		links = m_db.getLinks(linkType1Id);

		expectList = new ArrayList<UUID>();
		expectList.add(link1Rev1.getId());
		expectList.add(link2Rev1.getId());
		assertEquals(expectList, links);

		// Two links of specified type and one of another one
		Revision link3Rev1 = m_db.addLink(linkType2Id, obj1Id, obj1Rev1,
				obj2Id, obj2Rev1, null);

		links = m_db.getLinks(linkType1Id);

		expectList = new ArrayList<UUID>();
		expectList.add(link1Rev1.getId());
		expectList.add(link2Rev1.getId());
		assertEquals(expectList, links);

		links = m_db.getLinks(linkType2Id);

		expectList = new ArrayList<UUID>();
		expectList.add(link3Rev1.getId());
		assertEquals(expectList, links);

		// cleaning code
		try {
			m_db.deleteLink(link1Rev1.getId());
			m_db.deleteLink(link2Rev1.getId());
			m_db.deleteLink(link3Rev1.getId());
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testGetLinkId() throws ModelVersionDBException {
		// null type id
		try {
			m_db.getLinkId(null, obj1Id, ModelVersionDBService.LAST, obj2Id);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null source id
		try {
			m_db.getLinkId(linkType1Id, null, ModelVersionDBService.LAST, obj2Id);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null destination id
		try {
			m_db.getLinkId(linkType1Id, obj1Id, ModelVersionDBService.LAST, null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// No link 
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, null, true);
		m_db.setLinkSrcVersionSpecific(linkType1Id, true);
		m_db.setLinkDestVersionSpecific(linkType1Id, true);
		UUID linkId = m_db.getLinkId(linkType1Id, obj1Id, ModelVersionDBService.LAST, obj2Id);
		
		assertNull(linkId);

		// Link exists
		Revision link1Rev1 = m_db.addLink(linkType1Id, obj1Id, obj1Rev1,
				obj2Id, obj2Rev1, null);
		
		linkId = m_db.getLinkId(linkType1Id, obj1Id, obj1Rev1, obj2Id);
		assertEquals(link1Rev1.getId(), linkId);
		
		linkId = m_db.getLinkId(linkType1Id, obj1Id, ModelVersionDBService.LAST, obj2Id);
		assertEquals(link1Rev1.getId(), linkId);
		
		// ALL and ANY not allowed
		try {
			linkId = m_db.getLinkId(linkType1Id, obj1Id, ModelVersionDBService.ALL, obj2Id);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		try {
			linkId = m_db.getLinkId(linkType1Id, obj1Id, ModelVersionDBService.ANY, obj2Id);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Two links
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		Revision link2Rev1 = m_db.addLink(linkType1Id, obj2Id, obj1Rev2,
				obj2Id, obj2Rev2, null);
		
		linkId = m_db.getLinkId(linkType1Id, obj1Id, obj1Rev1, obj2Id);
		assertEquals(link1Rev1.getId(), linkId);
		
		linkId = m_db.getLinkId(linkType1Id, obj1Id, obj1Rev2, obj2Id);
		assertEquals(link1Rev1.getId(), linkId);
		
		linkId = m_db.getLinkId(linkType1Id, obj2Id, obj2Rev1, obj2Id);
		assertNull(linkId);
		
		linkId = m_db.getLinkId(linkType1Id, obj2Id, obj2Rev2, obj2Id);
		assertEquals(link2Rev1.getId(), linkId);

		// cleaning code
		try {
			m_db.deleteLink(link1Rev1.getId());
			m_db.deleteLink(link2Rev1.getId());
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testOutgoingLinksWithTypeId() throws ModelVersionDBException {
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		
		// null type id
		try {
			m_db.getOutgoingLinks(null, obj1Id, ModelVersionDBService.LAST);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null source id
		try {
			m_db.getOutgoingLinks(linkType1Id, null, ModelVersionDBService.LAST);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// No link 
		int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, null, true);
		m_db.setLinkSrcVersionSpecific(linkType1Id, true);
		m_db.setLinkDestVersionSpecific(linkType1Id, true);
		List<Revision> outgoingRevs = m_db.getOutgoingLinks(linkType1Id, obj1Id, ModelVersionDBService.LAST);
		
		assertNotNull(outgoingRevs);
		assertTrue(outgoingRevs.isEmpty());

		// One Link exists
		Revision link1Rev1 = m_db.addLink(linkType1Id, obj1Id, obj1Rev1,
				obj2Id, obj2Rev1, null);
		
		outgoingRevs = m_db.getOutgoingLinks(linkType1Id, obj1Id, obj1Rev1);
		List<Revision> expectRevs = new ArrayList<Revision>();
		expectRevs.add(link1Rev1);
		assertRevListMatch(expectRevs, outgoingRevs);
		
		outgoingRevs = m_db.getOutgoingLinks(linkType1Id, obj1Id, ModelVersionDBService.LAST);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(link1Rev1);
		assertRevListMatch(expectRevs, outgoingRevs);
		
		// ANY not allowed
		try {
			m_db.getOutgoingLinks(linkType1Id, obj1Id, ModelVersionDBService.ANY);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Many links
		int obj1Rev2 = m_db.createNewObjectRevision(obj1Id, obj1Rev1);
		int obj2Rev2 = m_db.createNewObjectRevision(obj2Id, obj2Rev1);
		Revision link1Rev2 = m_db.getLinkRev(linkType1Id, obj1Id, obj1Rev2, obj2Id, obj2Rev1);
		Revision link2Rev1 = m_db.addLink(linkType1Id, obj1Id, obj1Rev2,
				obj2Id, obj2Rev2, null);
		Revision link3Rev1 = m_db.addLink(linkType2Id, obj2Id, obj1Rev2,
				obj2Id, obj2Rev2, null);
		
		outgoingRevs = m_db.getOutgoingLinks(linkType1Id, obj1Id, obj1Rev1);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(link1Rev1);
		assertRevListMatch(expectRevs, outgoingRevs);
		
		outgoingRevs = m_db.getOutgoingLinks(linkType1Id, obj1Id, obj1Rev2);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(link1Rev2);
		expectRevs.add(link2Rev1);
		assertRevListMatch(expectRevs, outgoingRevs);
		
		// ALL allowed
		outgoingRevs = m_db.getOutgoingLinks(linkType1Id, obj1Id, ModelVersionDBService.ALL);
		expectRevs = new ArrayList<Revision>();
		expectRevs.add(link1Rev1);
		expectRevs.add(link1Rev2);
		expectRevs.add(link2Rev1);
		assertRevListMatch(expectRevs, outgoingRevs);

		// cleaning code
		try {
			m_db.deleteLink(link1Rev1.getId());
			m_db.deleteLink(link2Rev1.getId());
			m_db.deleteLink(link3Rev1.getId());
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	public void testOutgoingLinksWithDestId() throws ModelVersionDBException {
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		
		// null source id
		try {
			m_db.getOutgoingLinks(null, ModelVersionDBService.LAST, obj1Id);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		// null destination id
		try {
			m_db.getOutgoingLinks(obj1Id, ModelVersionDBService.LAST, null);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		throw new IllegalStateException("To Implement !!!");
	}
	
	public void testGetLinkState() throws ModelVersionDBException {
		// null link id
		try {
			m_db.getLinkState(null, ModelVersionDBService.LAST);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Link does not exist
		try {
			m_db.getLinkState(notExistLinkId, ModelVersionDBService.LAST);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// Link with empty state
		int obj1Rev1 = m_db.createObject(obj1Id, objType1Id, null, false);
		int obj2Rev1 = m_db.createObject(obj2Id, objType2Id, null, true);
		m_db.setLinkSrcVersionSpecific(linkType1Id, true);
		m_db.setLinkDestVersionSpecific(linkType1Id, true);
		Revision link1Rev1 = m_db.addLink(linkType1Id, obj1Id, obj1Rev1,
				obj2Id, obj2Rev1, null);

		Map<String, Object> stateMap = m_db.getLinkState(link1Rev1.getId(),
				link1Rev1.getRev());

		Map<String, Object> expectMap = new HashMap<String, Object>();
		assertEquals(expectMap, stateMap);

		// Link with one attribute
		Map<String, Object> link2StateMap = new HashMap<String, Object>();
		link2StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		m_db.setLinkState(link1Rev1.getId(), link1Rev1.getRev(), link2StateMap);

		stateMap = m_db.getLinkState(link1Rev1.getId(), link1Rev1.getRev());

		expectMap = link2StateMap;
		assertEquals(expectMap, stateMap);

		// Link with two attributes
		Map<String, Object> link3StateMap = new HashMap<String, Object>();
		link3StateMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		link3StateMap.put(ATTR2, new Integer(34));
		m_db.setLinkState(link1Rev1.getId(), link1Rev1.getRev(), link3StateMap);

		stateMap = m_db.getLinkState(link1Rev1.getId(), link1Rev1.getRev());

		expectMap = link3StateMap;
		assertEquals(expectMap, stateMap);

		// update values
		Map<String, Object> link4StateMap = new HashMap<String, Object>();
		link4StateMap.put(ATTR2, new Integer(1234567));
		m_db.setLinkState(link1Rev1.getId(), link1Rev1.getRev(), link4StateMap);

		stateMap = m_db.getLinkState(link1Rev1.getId(), link1Rev1.getRev());

		expectMap = new HashMap<String, Object>();
		expectMap.put(ATTR1, FIRST_ATTRIBUTE_VALUE);
		expectMap.put(ATTR2, link4StateMap.get(ATTR2));
		assertEquals(expectMap, stateMap);
		
		// ALL and ANY are not allowed
		try {
			m_db.getLinkState(link1Rev1.getId(), ModelVersionDBService.ALL);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}
		
		try {
			m_db.getLinkState(link1Rev1.getId(), ModelVersionDBService.ANY);

			fail();
		} catch (IllegalArgumentException e) {
			// PASSED
		}

		// cleaning code
		try {
			m_db.deleteLink(link1Rev1.getId());
			m_db.deleteObject(obj1Id);
			m_db.deleteObject(obj2Id);
		} catch (Exception e) {
			// ignore it
		}
	}
	
	/*
	 * 
	 * public void testDeleteLink() { // null link id try { ps.deleteLink(null);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Not existing object and link
	 * try { ps.deleteLink(notExistLinkId); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Existing object and link not exists try {
	 * ps.createObject(obj1Id, objType1Id, null); ps.deleteLink(obj1Id); fail(); }
	 * catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Existing link and obj not
	 * exist try { ps.createLink(link1Id, linkType1Id, link1SrcId, link1DestId,
	 * null); ps.deleteLink(link1Id); } catch (ModelVersionDBException e) {
	 * fail(); } // cleaning code try { ps.deleteObject(obj1Id); } catch
	 * (Exception e) { // ignore it } }
	 * 
	 * 
	 * private void assertSetEquals(List<UUID> linkList, List<UUID>
	 * expectList) { assertEquals(new HashSet<UUID>(expectList), new HashSet<UUID>(linkList)); }
	 * 
	 * public void testGetLinkValue() { // Link does not exist try {
	 * ps.getLinkValue(notExistLinkId, ATTR1); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // attribute does not exist try { ps.createLink(link1Id,
	 * linkType1Id, link1SrcId, link1DestId, null);
	 * 
	 * ps.getLinkValue(link1Id, ATTR1); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Check all attribute types try { Map<String, Object>
	 * link2StateMap = new HashMap<String, Object>(); link2StateMap.put(ATTR1,
	 * "first_attribute_value"); link2StateMap.put(ATTR2, null);
	 * link2StateMap.put(ATTR3, new Integer(1234)); link2StateMap.put(ATTR4, new
	 * Long(1234567890L)); link2StateMap.put(ATTR5, new
	 * java.util.Date(System.currentTimeMillis())); link2StateMap.put(ATTR6, new
	 * Ser("ser1")); link2StateMap.put(ATTR7, UUID.randomUUID());
	 * ps.createLink(link2Id, linkType2Id, link2SrcId, link2DestId,
	 * link2StateMap);
	 * 
	 * Object value = ps.getLinkValue(link2Id, ATTR1);
	 * assertEquals(link2StateMap.get(ATTR1), value);
	 * 
	 * value = ps.getLinkValue(link2Id, ATTR2);
	 * assertEquals(link2StateMap.get(ATTR2), value);
	 * 
	 * value = ps.getLinkValue(link2Id, ATTR3);
	 * assertEquals(link2StateMap.get(ATTR3), value);
	 * 
	 * value = ps.getLinkValue(link2Id, ATTR4);
	 * assertEquals(link2StateMap.get(ATTR4), value);
	 * 
	 * value = ps.getLinkValue(link2Id, ATTR5);
	 * assertEquals(link2StateMap.get(ATTR5), value);
	 * 
	 * value = ps.getLinkValue(link2Id, ATTR6);
	 * assertEquals(link2StateMap.get(ATTR6), value);
	 * 
	 * value = ps.getLinkValue(link2Id, ATTR7);
	 * assertEquals(link2StateMap.get(ATTR7), value); } catch
	 * (ModelVersionDBException e) { fail(); } // null link id try {
	 * ps.getLinkValue(null, ATTR1);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // null attribute name try {
	 * ps.getLinkValue(link2Id, null);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // cleaning code try {
	 * ps.deleteLink(link1Id); ps.deleteLink(link2Id); } catch (Exception e) { //
	 * ignore it } }
	 * 
	 * 
	 * public void testSetLinkValue() { // Link does not exist try {
	 * ps.setLinkValue(notExistLinkId, ATTR1, NEW_ATTR_VALUE); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // attribute value is compatible with previous attribute
	 * type try { Map<String, Object> link1StateMap = new HashMap<String,
	 * Object>(); link1StateMap.put(ATTR1, "first_attribute_value");
	 * ps.createLink(link1Id, linkType1Id, link1SrcId, link1DestId,
	 * link1StateMap);
	 * 
	 * ps.setLinkValue(link1Id, ATTR1, NEW_ATTR_VALUE); Object value =
	 * ps.getLinkValue(link1Id, ATTR1); assertEquals(NEW_ATTR_VALUE, value); }
	 * catch (ModelVersionDBException e) { fail(); } // attribute value is
	 * incompatible with previous attribute type but it is migratable // try to
	 * set a very long string value try { Map<String, Object> link1StateMap =
	 * new HashMap<String, Object>(); link1StateMap.put(ATTR1, NEW_ATTR_VALUE);
	 * link1StateMap.put(ATTR2, null); link1StateMap.put(ATTR3, null);
	 * link1StateMap.put(ATTR4, null); link1StateMap.put(ATTR5, null);
	 * link1StateMap.put(ATTR6, null); link1StateMap.put(ATTR7, null);
	 * ps.setLinkState(link1Id, link1StateMap); // new state map StringBuffer sb =
	 * new StringBuffer(); for (int i = 0 ; i < 10000; i++) sb.append("C");
	 * String veryLongStr = sb.toString();
	 * 
	 * Map<String, Object> expectedMap = new HashMap<String, Object>();
	 * expectedMap.put(ATTR1, veryLongStr); expectedMap.put(ATTR2,
	 * NEW_ATTR_VALUE); expectedMap.put(ATTR3, new Integer(1234));
	 * expectedMap.put(ATTR4, new Long(1234567890L)); expectedMap.put(ATTR5, new
	 * java.util.Date(System.currentTimeMillis())); expectedMap.put(ATTR6, new
	 * Ser("ser1")); expectedMap.put(ATTR7, new Boolean(false)); // string size
	 * migration ps.setLinkValue(link1Id, ATTR1, expectedMap.get(ATTR1)); //
	 * null to string migration ps.setLinkValue(link1Id, ATTR2,
	 * expectedMap.get(ATTR2)); // null to Integer migration
	 * ps.setLinkValue(link1Id, ATTR3, expectedMap.get(ATTR3)); // null to Long
	 * migration ps.setLinkValue(link1Id, ATTR4, expectedMap.get(ATTR4)); //
	 * null to Date migration ps.setLinkValue(link1Id, ATTR5,
	 * expectedMap.get(ATTR5)); // null to Serializable migration
	 * ps.setLinkValue(link1Id, ATTR6, expectedMap.get(ATTR6)); // null to
	 * Boolean migration ps.setLinkValue(link1Id, ATTR7,
	 * expectedMap.get(ATTR7)); // check state Map<String, Object>
	 * resultStatemap = ps.getLinkState(link1Id);
	 * 
	 * assertEquals(expectedMap, resultStatemap); } catch
	 * (ModelVersionDBException e) { fail(); } // attribute value is
	 * incompatible with previous attribute type try { ps.setLinkValue(link1Id,
	 * ATTR1, new Integer(1234)); fail(); } catch (ModelVersionDBException e) {
	 * if (!e.getMessage().startsWith("Found persist type ")) fail(); } // Check
	 * all attribute types try { Map<String, Object> link2StateMap = new
	 * HashMap<String, Object>(); link2StateMap.put(ATTR1,
	 * "first_attribute_value"); link2StateMap.put(ATTR2, null);
	 * link2StateMap.put(ATTR3, new Integer(1234)); link2StateMap.put(ATTR4, new
	 * Long(1234567890L)); link2StateMap.put(ATTR5, new
	 * java.util.Date(System.currentTimeMillis())); link2StateMap.put(ATTR6, new
	 * Ser("ser1")); link2StateMap.put(ATTR7, UUID.randomUUID());
	 * ps.createLink(link2Id, linkType2Id, link2SrcId, link2DestId,
	 * link2StateMap);
	 * 
	 * ps.setLinkValue(link2Id, ATTR1, NEW_ATTR_VALUE); Object value =
	 * ps.getLinkValue(link2Id, ATTR1); assertEquals(NEW_ATTR_VALUE, value);
	 * 
	 * Ser newSer = new Ser("serial"); ps.setLinkValue(link2Id, ATTR2, newSer);
	 * value = ps.getLinkValue(link2Id, ATTR2); assertEquals(newSer, value);
	 * 
	 * Integer newInt = new Integer(5555); ps.setLinkValue(link2Id, ATTR3,
	 * newInt); value = ps.getLinkValue(link2Id, ATTR3); assertEquals(newInt,
	 * value);
	 * 
	 * Long newLong = new Long(5555L); ps.setLinkValue(link2Id, ATTR4, newLong);
	 * value = ps.getLinkValue(link2Id, ATTR4); assertEquals(newLong, value);
	 * 
	 * java.util.Date newDate = new java.util.Date(System.currentTimeMillis());
	 * ps.setLinkValue(link2Id, ATTR5, newDate); value =
	 * ps.getLinkValue(link2Id, ATTR5); assertEquals(newDate, value);
	 * 
	 * newSer = new Ser("ser2"); ps.setLinkValue(link2Id, ATTR6, newSer); value =
	 * ps.getLinkValue(link2Id, ATTR6); assertEquals(newSer, value);
	 * 
	 * UUID newId = UUID.randomUUID(); ps.setLinkValue(link2Id, ATTR7, newId);
	 * value = ps.getLinkValue(link2Id, ATTR7); assertEquals(newId, value); }
	 * catch (ModelVersionDBException e) { fail(); } // Null object id try {
	 * ps.setLinkValue(null, ATTR1, NEW_ATTR_VALUE);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null attribute name try {
	 * ps.setLinkValue(link2Id, null, NEW_ATTR_VALUE);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // cleaning code try {
	 * ps.deleteLink(link1Id); ps.deleteLink(link2Id); } catch (Exception e) { //
	 * ignore it } }
	 * 
	 * public void testLinkExistsByTypeSrcDest() { // Null link id try {
	 * ps.linkExists(null);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Not existing link try {
	 * boolean linkExist = ps.linkExists(notExistLinkTypeId);
	 * assertFalse(linkExist); } catch (ModelVersionDBException e) { fail(); } //
	 * one link try { ps.createLink(link1Id, linkType1Id, link1SrcId,
	 * link1DestId, null);
	 * 
	 * boolean linkExist = ps.linkExists(linkType1Id, link1SrcId, link1DestId);
	 * assertTrue(linkExist); // source and destination toggled linkExist =
	 * ps.linkExists(linkType1Id, link1DestId, link1SrcId);
	 * assertFalse(linkExist); // bad source linkExist =
	 * ps.linkExists(linkType1Id, link2SrcId, link1DestId);
	 * assertFalse(linkExist); // bad destination linkExist =
	 * ps.linkExists(linkType1Id, link1SrcId, link2DestId);
	 * assertFalse(linkExist); // bad type linkExist =
	 * ps.linkExists(linkType2Id, link1SrcId, link2DestId);
	 * assertFalse(linkExist); } catch (ModelVersionDBException e) { fail(); } //
	 * one link try { ps.createLink(link2Id, linkType1Id, link1SrcId,
	 * link1DestId, null);
	 * 
	 * boolean linkExist = ps.linkExists(linkType1Id, link1SrcId, link1DestId);
	 * assertTrue(linkExist); } catch (ModelVersionDBException e) { fail(); } //
	 * cleaning code try { ps.deleteLink(link1Id); ps.deleteLink(link2Id); }
	 * catch (Exception e) { // ignore it } }
	 * 
	 * 
	 * public void testSetLinkState() { // Null object id try { Map<String,
	 * Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, "first_attribute_value"); ps.setLinkState(null,
	 * link1StateMap);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Link does not exist try { Map<String,
	 * Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, "first_attribute_value");
	 * ps.setLinkState(notExistLinkId, link1StateMap); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Empty State try { ps.createLink(link1Id, linkType1Id,
	 * link1SrcId, link1DestId, null);
	 * 
	 * Map<String, Object> link1StateMap = new HashMap<String, Object>();
	 * ps.setLinkState(link1Id, link1StateMap); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Null State try { ps.setLinkState(link1Id, null);
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // one attribute value is
	 * compatible with previous attribute type try { ps.setLinkValue(link1Id,
	 * ATTR1, "first_attribute_value");
	 * 
	 * Map<String, Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, NEW_ATTR_VALUE); ps.setLinkState(link1Id,
	 * link1StateMap);
	 * 
	 * Map<String, Object> stateMap = ps.getLinkState(link1Id);
	 * assertEquals(link1StateMap, stateMap); } catch (ModelVersionDBException
	 * e) { fail(); } // attribute value is incompatible with previous attribute
	 * type but it is migratable // try to set a very long string value Map<String,
	 * Object> newStateMap = new HashMap<String, Object>(); try { Map<String,
	 * Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, NEW_ATTR_VALUE); link1StateMap.put(ATTR2, null);
	 * link1StateMap.put(ATTR3, null); link1StateMap.put(ATTR4, null);
	 * link1StateMap.put(ATTR5, null); link1StateMap.put(ATTR6, null);
	 * link1StateMap.put(ATTR7, null); ps.setLinkState(link1Id, link1StateMap); //
	 * new state map StringBuffer sb = new StringBuffer(); for (int i = 0 ; i <
	 * 10000; i++) sb.append("C"); String veryLongStr = sb.toString();
	 * 
	 * newStateMap.put(ATTR1, veryLongStr); newStateMap.put(ATTR2,
	 * NEW_ATTR_VALUE); newStateMap.put(ATTR3, new Integer(1234));
	 * newStateMap.put(ATTR4, new Long(1234567890L)); newStateMap.put(ATTR5, new
	 * java.util.Date(System.currentTimeMillis())); newStateMap.put(ATTR6, new
	 * Ser("ser1")); newStateMap.put(ATTR7, new Boolean(false));
	 * ps.setLinkState(link1Id, newStateMap); // check state Map<String,
	 * Object> resultStatemap = ps.getLinkState(link1Id);
	 * 
	 * assertEquals(newStateMap, resultStatemap); } catch
	 * (ModelVersionDBException e) { fail(); } // attribute value is
	 * incompatible with previous attribute type try { Map<String, Object>
	 * link1StateMap = new HashMap<String, Object>(); link1StateMap.put(ATTR1,
	 * new Integer(1234));
	 * 
	 * ps.setLinkState(link1Id, link1StateMap); fail(); } catch
	 * (ModelVersionDBException e) { if (!e.getMessage().startsWith("Found
	 * persist type ")) fail(); } // set a new attribute try {
	 * newStateMap.put(ATTR1, NEW_ATTR_VALUE); ps.setLinkValue(link1Id, ATTR1,
	 * newStateMap.get(ATTR1));
	 * 
	 * newStateMap.put(ATTR2, "yet another value"); Map<String, Object>
	 * link1StateMap = new HashMap<String, Object>(); link1StateMap.put(ATTR2,
	 * newStateMap.get(ATTR2));
	 * 
	 * ps.setLinkState(link1Id, link1StateMap); Map<String, Object> stateMap =
	 * ps.getLinkState(link1Id); link1StateMap.put(ATTR1, NEW_ATTR_VALUE);
	 * assertEquals(newStateMap, stateMap); } catch (ModelVersionDBException e) {
	 * fail(); } // Check all attribute types try { Map<String, Object>
	 * link2StateMap = new HashMap<String, Object>(); link2StateMap.put(ATTR1,
	 * "first_attribute_value"); link2StateMap.put(ATTR2, null);
	 * link2StateMap.put(ATTR3, new Integer(1234)); link2StateMap.put(ATTR4, new
	 * Long(1234567890L)); link2StateMap.put(ATTR5, new
	 * java.util.Date(System.currentTimeMillis())); link2StateMap.put(ATTR6, new
	 * Ser("ser1")); ps.createLink(link2Id, linkType2Id, link2SrcId,
	 * link2DestId, link2StateMap);
	 * 
	 * Ser newSer = new Ser("serial"); link2StateMap.put(ATTR1, NEW_ATTR_VALUE);
	 * link2StateMap.put(ATTR2, newSer); link2StateMap.put(ATTR3, new
	 * Integer(5555)); link2StateMap.put(ATTR4, new Long(5555L));
	 * link2StateMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
	 * link2StateMap.put(ATTR6, new Ser("ser2")); ps.setLinkState(link2Id,
	 * link2StateMap); Map<String, Object> stateMap = ps.getLinkState(link2Id);
	 * assertEquals(link2StateMap, stateMap); } catch (ModelVersionDBException
	 * e) { fail(); } // cleaning code try { ps.deleteLink(link1Id);
	 * ps.deleteLink(link2Id); } catch (Exception e) { // ignore it } }
	 * 
	 * 
	 * 
	 * public void testCreateLink() { // Null link id try { Map<String, Object>
	 * link1StateMap = new HashMap<String, Object>(); link1StateMap.put(ATTR1,
	 * NEW_ATTR_VALUE);
	 * 
	 * ps.createLink(null, linkType1Id, link1SrcId, link1DestId, link1StateMap);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null type id try { Map<String,
	 * Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, NEW_ATTR_VALUE);
	 * 
	 * ps.createLink(link1Id, null, link1SrcId, link1DestId, link1StateMap);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null source id try { Map<String,
	 * Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, NEW_ATTR_VALUE);
	 * 
	 * ps.createLink(link1Id, linkType1Id, null, link1DestId, link1StateMap);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null destination id try { Map<String,
	 * Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, NEW_ATTR_VALUE);
	 * 
	 * ps.createLink(link1Id, linkType1Id, link1SrcId, null, link1StateMap);
	 * 
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // null stateMap try {
	 * ps.createLink(link1Id, linkType1Id, link1SrcId, link1DestId, null); Map<String,
	 * Object> link1StateMap = ps.getLinkState(link1Id);
	 * assertTrue(link1StateMap.isEmpty()); assertEquals(linkType1Id,
	 * ps.getLinkType(link1Id)); assertEquals(link1SrcId,
	 * ps.getLinkSrc(link1Id)); assertEquals(link1DestId,
	 * ps.getLinkDest(link1Id)); } catch (ModelVersionDBException e) { fail(); } //
	 * Empty State try { ps.deleteLink(link1Id); ps.createLink(link1Id,
	 * linkType1Id, link1SrcId, link1DestId, new HashMap<String, Object>());
	 * 
	 * Map<String, Object> link1StateMap = ps.getLinkState(link1Id);
	 * assertTrue(link1StateMap.isEmpty()); assertEquals(linkType1Id,
	 * ps.getLinkType(link1Id)); assertEquals(link1SrcId,
	 * ps.getLinkSrc(link1Id)); assertEquals(link1DestId,
	 * ps.getLinkDest(link1Id)); } catch (ModelVersionDBException e) { fail(); } //
	 * Existing link try { ps.createLink(link1Id, linkType1Id, link1SrcId,
	 * link1DestId, null); fail(); } catch (IllegalArgumentException e) { //
	 * PASSED } catch (ModelVersionDBException e) { fail(); } // one attribute
	 * try { ps.deleteLink(link1Id);
	 * 
	 * Map<String, Object> link1StateMap = new HashMap<String, Object>();
	 * link1StateMap.put(ATTR1, NEW_ATTR_VALUE); ps.createLink(link1Id,
	 * linkType1Id, link1SrcId, link1DestId, link1StateMap);
	 * 
	 * Map<String, Object> stateMap = ps.getLinkState(link1Id);
	 * assertEquals(link1StateMap, stateMap); } catch (ModelVersionDBException
	 * e) { fail(); } // Check all attribute types try { Map<String, Object>
	 * link2StateMap = new HashMap<String, Object>(); link2StateMap.put(ATTR1,
	 * "first_attribute_value"); link2StateMap.put(ATTR2, null);
	 * link2StateMap.put(ATTR3, new Integer(1234)); link2StateMap.put(ATTR4, new
	 * Long(1234567890L)); link2StateMap.put(ATTR5, new
	 * java.util.Date(System.currentTimeMillis())); link2StateMap.put(ATTR6, new
	 * Ser("ser1")); ps.createLink(link2Id, linkType2Id, link2SrcId,
	 * link2DestId, link2StateMap);
	 * 
	 * Map<String, Object> stateMap = ps.getLinkState(link2Id);
	 * assertEquals(link2StateMap, stateMap); } catch (ModelVersionDBException
	 * e) { fail(); } // cleaning code try { ps.deleteLink(link1Id);
	 * ps.deleteLink(link2Id); } catch (Exception e) { // ignore it } }
	 * 
	 * public void testSetSrcLink() { // Not existing link try {
	 * ps.setLinkSrc(notExistLinkTypeId, link1SrcId); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // link exists try { ps.createLink(link1Id, linkType1Id,
	 * link1SrcId, link1DestId, null);
	 * 
	 * ps.setLinkSrc(link1Id, link2SrcId);
	 * 
	 * UUID srcId = ps.getLinkSrc(link1Id); assertEquals(link2SrcId, srcId); }
	 * catch (ModelVersionDBException e) { fail(); } // Null type id try {
	 * ps.setLinkSrc(null, link1SrcId); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Null source id try { ps.setLinkSrc(linkType1Id, null);
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // cleaning code try {
	 * ps.deleteLink(link1Id); } catch (Exception e) { // ignore it } }
	 * 
	 * public void testSetDestLink() { // Not existing link try {
	 * ps.setLinkDest(notExistLinkTypeId, link1SrcId); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // link exists try { ps.createLink(link1Id, linkType1Id,
	 * link1SrcId, link1DestId, null);
	 * 
	 * ps.setLinkDest(link1Id, link2DestId);
	 * 
	 * UUID destId = ps.getLinkDest(link1Id); assertEquals(link2DestId, destId); }
	 * catch (ModelVersionDBException e) { fail(); } // Null type id try {
	 * ps.setLinkDest(null, link1DestId); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Null destination id try { ps.setLinkDest(linkType1Id,
	 * null); fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // cleaning code try {
	 * ps.deleteLink(link1Id); } catch (Exception e) { // ignore it } }
	 * 
	 * public void testGetSrcLink() { // Not existing link try {
	 * ps.getLinkSrc(notExistLinkTypeId); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // link exists try { ps.createLink(link1Id, linkType1Id,
	 * link1SrcId, link1DestId, null);
	 * 
	 * UUID srcId = ps.getLinkSrc(link1Id); assertEquals(link1SrcId, srcId); //
	 * after update of source ps.setLinkSrc(link1Id, link2SrcId);
	 * 
	 * srcId = ps.getLinkSrc(link1Id); assertEquals(link2SrcId, srcId); } catch
	 * (ModelVersionDBException e) { fail(); } // Null link id try {
	 * ps.getLinkSrc(null); fail(); } catch (IllegalArgumentException e) { //
	 * PASSED } catch (ModelVersionDBException e) { fail(); } // cleaning code
	 * try { ps.deleteLink(link1Id); } catch (Exception e) { // ignore it } }
	 * 
	 * public void testGetDestLink() { // Not existing link try {
	 * ps.getLinkDest(notExistLinkTypeId); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // link exists try { ps.createLink(link1Id, linkType1Id,
	 * link1SrcId, link1DestId, null);
	 * 
	 * UUID destId = ps.getLinkDest(link1Id); assertEquals(link1DestId, destId);
	 * 
	 * ps.setLinkDest(link1Id, link2DestId);
	 * 
	 * destId = ps.getLinkDest(link1Id); assertEquals(link2DestId, destId); }
	 * catch (ModelVersionDBException e) { fail(); } // Null link id try {
	 * ps.getLinkDest(null); fail(); } catch (IllegalArgumentException e) { //
	 * PASSED } catch (ModelVersionDBException e) { fail(); } // cleaning code
	 * try { ps.deleteLink(link1Id); } catch (Exception e) { // ignore it } }
	 * 
	 * public void testGetSrcLinkWithType() { // One link try {
	 * ps.createLink(link1Id, linkType1Id, link1SrcId, link1DestId, null); //
	 * Not existing link type List<UUID> srcList = ps.getLinkSrc(link1DestId,
	 * notExistLinkTypeId);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>();
	 * assertSetEquals(expectList, srcList); // not existing dest srcList =
	 * ps.getLinkSrc(notExistObjId, linkType1Id);
	 * 
	 * expectList = new ArrayList<UUID>(); assertSetEquals(expectList,
	 * srcList); // one src srcList = ps.getLinkSrc(link1DestId, linkType1Id);
	 * 
	 * expectList = new ArrayList<UUID>(); expectList.add(link1SrcId);
	 * assertSetEquals(expectList, srcList); } catch (ModelVersionDBException e) {
	 * fail(); } // two sources try { ps.createLink(link2Id, linkType1Id,
	 * link2SrcId, link1DestId, null);
	 * 
	 * List<UUID> srcList = ps.getLinkSrc(link1DestId, linkType1Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>();
	 * expectList.add(link1SrcId); expectList.add(link2SrcId);
	 * assertSetEquals(expectList, srcList); } catch (ModelVersionDBException e) {
	 * fail(); } // Null destination id try { ps.getLinkSrc(null, linkType1Id);
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null type id try {
	 * ps.getLinkSrc(link1DestId, null); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // cleaning code try { ps.deleteLink(link1Id);
	 * ps.deleteLink(link2Id); } catch (Exception e) { // ignore it } }
	 * 
	 * public void testGetDestLinkWithType() { // One link try {
	 * ps.createLink(link1Id, linkType1Id, link1SrcId, link1DestId, null); //
	 * Not existing link type List<UUID> destList = ps.getLinkDest(link1SrcId,
	 * notExistLinkTypeId);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>(); assertEquals(expectList,
	 * destList); // not existing dest destList = ps.getLinkDest(notExistObjId,
	 * linkType1Id);
	 * 
	 * expectList = new ArrayList<UUID>(); assertEquals(expectList, destList); //
	 * one src destList = ps.getLinkDest(link1SrcId, linkType1Id);
	 * 
	 * expectList = new ArrayList<UUID>(); expectList.add(link1DestId);
	 * assertEquals(expectList, destList); } catch (ModelVersionDBException e) {
	 * fail(); } // two links try { ps.createLink(link2Id, linkType1Id,
	 * link1SrcId, link2DestId, null);
	 * 
	 * List<UUID> destList = ps.getLinkDest(link1SrcId, linkType1Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>();
	 * expectList.add(link1DestId); expectList.add(link2DestId);
	 * assertEquals(expectList, destList); } catch (ModelVersionDBException e) {
	 * fail(); } // Null source id try { ps.getLinkDest(null, linkType1Id);
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null type id try {
	 * ps.getLinkDest(link1SrcId, null); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // cleaning code try { ps.deleteLink(link1Id);
	 * ps.deleteLink(link2Id); } catch (Exception e) { // ignore it } }
	 * 
	 * public void testGetLinkWithTypeIdAndSrcAndDest() { // No link in base try {
	 * UUID link = ps.getLink(linkType1Id, obj1Id, obj2Id);
	 * 
	 * UUID expectId = null; assertEquals(expectId, link); } catch
	 * (ModelVersionDBException e) { fail(); } // link type not exist in base
	 * try { UUID link = ps.getLink(notExistLinkTypeId, obj1Id, obj2Id);
	 * 
	 * UUID expectId = null; assertEquals(expectId, link); } catch
	 * (ModelVersionDBException e) { fail(); } // link exist try {
	 * ps.createLink(link1Id, linkType1Id, obj1Id, obj2Id, null);
	 * 
	 * UUID link = ps.getLink(linkType1Id, obj1Id, obj2Id);
	 * 
	 * UUID expectId = link1Id; assertEquals(expectId, link); } catch
	 * (ModelVersionDBException e) { fail(); } // bad link type try { UUID link =
	 * ps.getLink(linkType2Id, obj1Id, obj2Id);
	 * 
	 * UUID expectId = null; assertEquals(expectId, link); } catch
	 * (ModelVersionDBException e) { fail(); } // bad source try { UUID link =
	 * ps.getLink(linkType1Id, notExistObjId, obj2Id);
	 * 
	 * UUID expectId = null; assertEquals(expectId, link); } catch
	 * (ModelVersionDBException e) { fail(); } // bad dest try { UUID link =
	 * ps.getLink(linkType1Id, obj1Id, notExistObjId);
	 * 
	 * UUID expectId = null; assertEquals(expectId, link); } catch
	 * (ModelVersionDBException e) { fail(); } // Null type id try {
	 * ps.getLink(null, obj1Id, obj2Id); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Null source id try { ps.getLink(linkType1Id, null,
	 * obj2Id); fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null destination id try {
	 * ps.getLink(linkType1Id, obj1Id, null); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // cleaning code try { ps.deleteLink(link1Id); } catch
	 * (Exception e) { // ignore it } }
	 * 
	 * public void testGetObjectsWithTypeIdAndAttrAndAttrVal() { Map<String,
	 * Object> attrMap = new HashMap<String, Object>(); attrMap.put(ATTR1,
	 * "first_attribute_value"); attrMap.put(ATTR2, null); attrMap.put(ATTR3,
	 * new Integer(1234)); attrMap.put(ATTR4, new Long(1234567890L));
	 * attrMap.put(ATTR5, new java.util.Date(System.currentTimeMillis()));
	 * attrMap.put(ATTR6, new Ser("ser1")); // Null type id try {
	 * ps.getObjects(null, ATTR1, attrMap.get(ATTR1)); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Null attribute name try { ps.getObjects(objType1Id,
	 * null, attrMap.get(ATTR1)); fail(); } catch (IllegalArgumentException e) { //
	 * PASSED } catch (ModelVersionDBException e) { fail(); } // No object in
	 * base try { Set<UUID> objSet = ps.getObjects(objType1Id, ATTR1,
	 * attrMap.get(ATTR1));
	 * 
	 * Set<UUID> expectSet = new HashSet<UUID>(); assertEquals(expectSet,
	 * objSet); } catch (ModelVersionDBException e) { fail(); } // One object
	 * try { ps.createObject(obj1Id, objType1Id, attrMap);
	 * 
	 * Set<UUID> objSet = ps.getObjects(objType1Id, ATTR1, attrMap.get(ATTR1));
	 * 
	 * Set<UUID> expectSet = new HashSet<UUID>(); expectSet.add(obj1Id);
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR2, attrMap.get(ATTR2));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR3, attrMap.get(ATTR3));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR4, attrMap.get(ATTR4));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR5, attrMap.get(ATTR5));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR6, attrMap.get(ATTR6));
	 * assertEquals(expectSet, objSet); } catch (ModelVersionDBException e) {
	 * fail(); } // bad object type try { Set<UUID> objSet =
	 * ps.getObjects(objType2Id, ATTR1, attrMap.get(ATTR1));
	 * 
	 * Set<UUID> expectSet = new HashSet<UUID>(); assertEquals(expectSet,
	 * objSet); } catch (ModelVersionDBException e) { fail(); } // two objects
	 * try { ps.createObject(obj2Id, objType1Id, attrMap);
	 * 
	 * Set<UUID> objSet = ps.getObjects(objType1Id, ATTR1, attrMap.get(ATTR1));
	 * 
	 * Set<UUID> expectSet = new HashSet<UUID>(); expectSet.add(obj1Id);
	 * expectSet.add(obj2Id); assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR2, attrMap.get(ATTR2));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR3, attrMap.get(ATTR3));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR4, attrMap.get(ATTR4));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR5, attrMap.get(ATTR5));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR6, attrMap.get(ATTR6));
	 * assertEquals(expectSet, objSet); } catch (ModelVersionDBException e) {
	 * fail(); } // 3 objects try { Map<String, Object> attrMap2 = new HashMap<String,
	 * Object>(); attrMap2.put(ATTR1, attrMap.get(ATTR1)); attrMap2.put(ATTR2,
	 * new Ser("ser2")); attrMap2.put(ATTR3, new Integer(4321));
	 * attrMap2.put(ATTR4, new Long(123L)); attrMap2.put(ATTR5, new
	 * java.util.Date(System.currentTimeMillis() + 1)); attrMap2.put(ATTR6, new
	 * Ser("ser3"));
	 * 
	 * ps.createObject(obj3Id, objType1Id, attrMap2);
	 * 
	 * Set<UUID> objSet = ps.getObjects(objType1Id, ATTR1, attrMap.get(ATTR1));
	 * 
	 * Set<UUID> expectSet = new HashSet<UUID>(); expectSet.add(obj1Id);
	 * expectSet.add(obj2Id); expectSet.add(obj3Id); assertEquals(expectSet,
	 * objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR2, attrMap.get(ATTR2)); expectSet =
	 * new HashSet<UUID>(); expectSet.add(obj1Id); expectSet.add(obj2Id);
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR3, attrMap.get(ATTR3));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR4, attrMap.get(ATTR4));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR5, attrMap.get(ATTR5));
	 * assertEquals(expectSet, objSet);
	 * 
	 * 
	 * objSet = ps.getObjects(objType1Id, ATTR6, attrMap.get(ATTR6));
	 * assertEquals(expectSet, objSet); } catch (ModelVersionDBException e) {
	 * fail(); } // cleaning code try { ps.deleteObject(obj1Id);
	 * ps.deleteObject(obj2Id); ps.deleteObject(obj3Id); } catch (Exception e) { //
	 * ignore it } }
	 * 
	 * public void testGetLinksWithSrcAndDest() { // No link in base try { List<UUID>
	 * linkList = ps.getLinks(obj1Id, obj2Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>();
	 * assertSetEquals(expectList, linkList); } catch (ModelVersionDBException
	 * e) { fail(); } // link exist try { ps.createLink(link1Id, linkType1Id,
	 * obj1Id, obj2Id, null); List<UUID> linkList = ps.getLinks(obj1Id,
	 * obj2Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>(); expectList.add(link1Id);
	 * assertSetEquals(expectList, linkList); } catch (ModelVersionDBException
	 * e) { fail(); } // two link exists try { ps.createLink(link2Id,
	 * linkType2Id, obj1Id, obj2Id, null); List<UUID> linkList =
	 * ps.getLinks(obj1Id, obj2Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>(); expectList.add(link1Id);
	 * expectList.add(link2Id); assertSetEquals(expectList, linkList); } catch
	 * (ModelVersionDBException e) { fail(); } // bad source try { List<UUID>
	 * linkList = ps.getLinks(notExistObjId, obj2Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>();
	 * assertSetEquals(expectList, linkList); } catch (ModelVersionDBException
	 * e) { fail(); } // bad destination try { List<UUID> linkList =
	 * ps.getLinks(obj1Id, notExistObjId);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>();
	 * assertSetEquals(expectList, linkList); } catch (ModelVersionDBException
	 * e) { fail(); } // Null source id try { ps.getLinks(null, obj2Id); fail(); }
	 * catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // Null destination id try {
	 * ps.getLinks(obj1Id, null); fail(); } catch (IllegalArgumentException e) { //
	 * PASSED } catch (ModelVersionDBException e) { fail(); } // cleaning code
	 * try { ps.deleteLink(link1Id); ps.deleteLink(link2Id); } catch (Exception
	 * e) { // ignore it } }
	 * 
	 * public void testGetOutgoingLinks() { // No link in base try { List<UUID>
	 * linkList = ps.getOugoingLinks(obj1Id, linkType1Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>(); assertEquals(expectList,
	 * linkList); } catch (ModelVersionDBException e) { fail(); } // link exist
	 * try { ps.createLink(link1Id, linkType1Id, obj1Id, obj2Id, null); List<UUID>
	 * linkList = ps.getOugoingLinks(obj1Id, linkType1Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>(); expectList.add(link1Id);
	 * assertEquals(expectList, linkList); } catch (ModelVersionDBException e) {
	 * fail(); } // two link exists try { ps.createLink(link2Id, linkType2Id,
	 * obj1Id, obj2Id, null); List<UUID> linkList = ps.getOugoingLinks(obj1Id,
	 * linkType1Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>(); expectList.add(link1Id);
	 * assertEquals(expectList, linkList);
	 * 
	 * linkList = ps.getOugoingLinks(obj1Id, linkType2Id);
	 * 
	 * expectList = new ArrayList<UUID>(); expectList.add(link2Id);
	 * assertEquals(expectList, linkList); } catch (ModelVersionDBException e) {
	 * fail(); } // source not exist try { List<UUID> linkList =
	 * ps.getOugoingLinks(notExistObjId, linkType1Id);
	 * 
	 * List<UUID> expectList = new ArrayList<UUID>(); assertEquals(expectList,
	 * linkList); } catch (ModelVersionDBException e) { fail(); } // Null source
	 * id try { ps.getOugoingLinks(null, linkType1Id); fail(); } catch
	 * (IllegalArgumentException e) { // PASSED } catch (ModelVersionDBException
	 * e) { fail(); } // Null type id try { ps.getOugoingLinks(obj1Id, null);
	 * fail(); } catch (IllegalArgumentException e) { // PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // cleaning code try {
	 * ps.deleteLink(link1Id); ps.deleteLink(link2Id); } catch (Exception e) { //
	 * ignore it } }
	 * 
	 * 
	 * 
	 * public void testReOrderLinks() { // null type try { UUID[] linkIds = {
	 * link1Id, link2Id }; ps.reOrderLinks(null, link1SrcId, linkIds);
	 * 
	 * fail(); } catch (IllegalArgumentException iae) { //PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // null source id try { UUID[]
	 * linkIds = { link1Id, link2Id }; ps.reOrderLinks(linkType1Id, null,
	 * linkIds);
	 * 
	 * fail(); } catch (IllegalArgumentException iae) { //PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // null linkIds try {
	 * ps.reOrderLinks(linkType1Id, link1SrcId, null);
	 * 
	 * fail(); } catch (IllegalArgumentException iae) { //PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // link list size < 2 try {
	 * UUID[] linkIds = { link1Id }; ps.reOrderLinks(linkType1Id, link1SrcId,
	 * linkIds);
	 * 
	 * fail(); } catch (IllegalArgumentException iae) { //PASSED } catch
	 * (ModelVersionDBException e) { fail(); } // 2 links try {
	 * ps.createLink(link1Id, linkType1Id, link1SrcId, link1DestId, null);
	 * ps.createLink(link2Id, linkType1Id, link1SrcId, link2DestId, null);
	 * 
	 * UUID[] linkIds = { link2Id, link1Id }; ps.reOrderLinks(linkType1Id,
	 * link1SrcId, linkIds);
	 * 
	 * List<UUID> destIds = ps.getLinkDest(link1SrcId, linkType1Id); List<UUID>
	 * expectList = new ArrayList<UUID>(); expectList.add(link2DestId);
	 * expectList.add(link1DestId); assertEquals(expectList, destIds); } catch
	 * (ModelVersionDBException e) { fail(); } // 3 links try {
	 * ps.createLink(link3Id, linkType1Id, link1SrcId, link3DestId, null);
	 * 
	 * UUID[] linkIds = { link3Id, link2Id }; ps.reOrderLinks(linkType1Id,
	 * link1SrcId, linkIds);
	 * 
	 * List<UUID> destIds = ps.getLinkDest(link1SrcId, linkType1Id); List<UUID>
	 * expectList = new ArrayList<UUID>(); expectList.add(link3DestId);
	 * expectList.add(link2DestId); expectList.add(link1DestId);
	 * assertEquals(expectList, destIds); } catch (ModelVersionDBException e) {
	 * fail(); } // 4 links try { ps.createLink(link4Id, linkType1Id,
	 * link1SrcId, link4DestId, null);
	 * 
	 * UUID[] linkIds = { link2Id, link4Id, link3Id };
	 * ps.reOrderLinks(linkType1Id, link1SrcId, linkIds);
	 * 
	 * List<UUID> destIds = ps.getLinkDest(link1SrcId, linkType1Id); List<UUID>
	 * expectList = new ArrayList<UUID>(); expectList.add(link2DestId);
	 * expectList.add(link4DestId); expectList.add(link3DestId);
	 * assertTrue(destIds.indexOf(link1DestId) > 0);
	 * destIds.remove(link1DestId); assertEquals(expectList, destIds); } catch
	 * (ModelVersionDBException e) { fail(); } // cleaning code try {
	 * ps.deleteLink(link1Id); ps.deleteLink(link2Id); ps.deleteLink(link3Id);
	 * ps.deleteLink(link4Id); } catch (Exception e) { // ignore it } }
	 */

	private void assertSetEquals(List<UUID> linkList, 
			List<UUID> expectList) { 
		assertEquals(new HashSet<UUID>(expectList), new HashSet<UUID>(linkList)); 
	}
	
	private void assertContainsRevs(int[] revs, int... expectRevs) {
		assertEquals(revs.length, expectRevs.length);
		for (int expectRev : expectRevs) 
			assertContains("Miss revision " + expectRev, revs, expectRev);
	}
	
	private void checkLinkRevs(UUID linkType, UUID srcId, int srcRev,
			CheckRevision... expectDestRevs) throws ModelVersionDBException {
		assertEquals(expectDestRevs.length, m_db.getLinkNumber(linkType, srcId,
				srcRev));

		List<Revision> destRevs = m_db.getLinkDestRev(linkType, srcId, srcRev);
		assertNotNull(destRevs);
		assertEquals(expectDestRevs.length, destRevs.size());

		for (CheckRevision expectDestRev : expectDestRevs) {
			UUID destId = expectDestRev.getId();
			int destRev = expectDestRev.getRev();
			checkFindRev(destRevs, destId, destRev);

			Revision linkRev = m_db.getLinkRev(linkType, srcId, srcRev, destId,
					destRev);
			assertEquals(expectDestRev.getStateMap(), m_db.getLinkState(linkRev
					.getId(), linkRev.getRev()));
		}
	}
	
	private void checkFindRev(List<Revision> revs, UUID objId,
			int objRev) {
		if (revs == null)
			fail("Revision list cannot be null.");
		
		for (Revision rev : revs) {
			if (objId.equals(rev.getId()) &&
					(objRev == rev.getRev()))
				return;
		}
		
		fail("Revision " + objRev + " of object " + objId + " canot be found.");
	}

	private void assertRevListMatch(List<Revision> expectRevs,
			List<Revision> revs) {
		if (expectRevs == null) {
			assertEquals(expectRevs, revs);
		}
		if (revs == null)
			fail();
		
		assertEquals("Revision lists must have same size.", expectRevs.size(), revs.size());
		
		for (UUID objectId : getObjectIds(expectRevs)) {
			int expectIdx = findFirstRev(objectId, expectRevs);
			int idx = findFirstRev(objectId, revs);
			
			if (idx == -1)
				fail("There is no revision of " + objectId + ".");
			
			for (int j = expectIdx; j < expectRevs.size(); j++) {
				Revision expectRev = expectRevs.get(j);
				if (!objectId.equals(expectRev.getId()))
					break;
				
				int expectRevNb = expectRev.getRev();
				
				int revIdx = idx + (j - expectIdx);
				if (revIdx >= revs.size())
					fail("Revision " + expectRevNb + " of object " + objectId + 
							" is missing or revision list is not sorted by rev number per each object.");
				Revision rev = revs.get(revIdx);
				assertEquals("Revision " + expectRevNb + " of object " + objectId + 
						" is missing or revision list is not sorted by rev number per each object.", 
						expectRevNb, rev.getRev());
				
				UUID expectTypeId = expectRev.getTypeId();
				UUID typeId = rev.getTypeId();
				assertEquals("Revision " + expectRevNb + " of object " + objectId + 
						" has object type " + typeId + " instead of " + expectTypeId + ".", expectTypeId, typeId);
			}
		}
	}

	private int findFirstRev(UUID objectId, List<Revision> revs) {
		for (int i = 0; i < revs.size(); i++) {
			if (revs.get(i).getId().equals(objectId))
				return i;
		}
		
		return -1;
	}

	private Set<UUID> getObjectIds(List<Revision> expectRevs) {
		Set<UUID> objectIds = new HashSet<UUID>();
		
		for (Revision rev : expectRevs) {
			objectIds.add(rev.getId());
		}
		
		return objectIds;
	}

	private String getHSQLServerURL(String dbName, int port) {
		return "jdbc:hsqldb:mem:" + dbName;
	}

	private Server createHSQLServer(String dbName, int port) {
		Server server = new Server();
		server.putPropertiesFromString("database.0=mem:" + dbName + ";sql.enforce_strict_size=true");
		server.setLogWriter(null);
		server.setErrWriter(null);
		server.setPort(port);
		server.start();
		
		return server;
	}
	
	private String getBaseType() {
		String[] urlParts = _url.split(":");
		if (urlParts.length < 2)
			return null;
		
		String jdbcType = urlParts[1];
		String baseType = null;
		if (jdbcType.equals("hsqldb")) {
			if ((urlParts.length > 2) && (urlParts[2].equalsIgnoreCase("mem")))
				baseType = ModelVersionDBService.HSQL_IN_MEMORY_TYPE;
			else
				baseType = ModelVersionDBService.HSQL_TYPE;
		}
		if (jdbcType.equals("mysql")) {
			baseType = ModelVersionDBService.MYSQL_TYPE;
		}
		if (jdbcType.startsWith("oracle")) {
			baseType = ModelVersionDBService.ORACLE_TYPE;
		}
		
		return baseType;
	}
}
