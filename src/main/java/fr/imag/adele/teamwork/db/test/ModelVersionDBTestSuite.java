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

import org.osgi.framework.BundleContext;

public class ModelVersionDBTestSuite {

	public static Test suite(BundleContext bc) {
        DBOSGiTestSuite ots = new DBOSGiTestSuite("Tests of ModelVersionDBService", bc);
        
//        DBOSGiTestSuite mysqlTestSuite = new DBOSGiTestSuite("Tests of ModelVersionDBService with MySQL", bc);
//        mysqlTestSuite.addTestSuite(ModelVersionDBTestCase.class, "mysql", "localhost", 3306, "TestModelDB", "root", "ertdfg");
//        ots.addTestSuite(mysqlTestSuite);
//        
//        DBOSGiTestSuite oracleTestSuite = new DBOSGiTestSuite("Tests of ModelVersionDBService with Oracle", bc);
//        oracleTestSuite.addTestSuite(ModelVersionDBTestCase.class, "oracle:thin", "localhost", 1521, "XE", "thomas", "thomasPwd");
//        ots.addTestSuite(oracleTestSuite);
        
        DBOSGiTestSuite hsqlTestSuite = new DBOSGiTestSuite("Tests of ModelVersionDBService with HSQLDB", bc);
        hsqlTestSuite.addTestSuite(ModelVersionDBTestCase.class, "hsqldb:hsql", "localhost", 9002, "TestModelDB", "sa", "");
        ots.addTestSuite(hsqlTestSuite);
        
        return ots;
    }
}
