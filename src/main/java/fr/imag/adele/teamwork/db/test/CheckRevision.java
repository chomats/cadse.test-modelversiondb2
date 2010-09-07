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
 *
 * Copyright (C) 2006-2010 Adele Team/LIG/Grenoble University, France
 */
package fr.imag.adele.teamwork.db.test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CheckRevision {

	private int _rev;
	private UUID _objId;
	private Map<String, Object> _stateMap;
	
	public CheckRevision(UUID objId, int rev, Map<String, Object> stateMap) {
		_rev = rev;
		_objId = objId;
		_stateMap = stateMap;
	}
	
	public int getRev() {
		return _rev;
	}
	
	public UUID getId() {
		return _objId;
	}

	public Map<String, Object> getStateMap() {
		if (_stateMap == null)
			return new HashMap<String, Object>();
		return _stateMap;
	}
	
	@Override
	public int hashCode() {
		return _rev + _objId.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof CheckRevision))
			return false;

		CheckRevision rev = (CheckRevision) obj;
		
		return (_objId.equals(rev.getId())) && (_rev == rev.getRev()) &&
		       same(_stateMap, rev.getStateMap());
	}
	
	public CheckRevision getRev(int revNb) {
		return new CheckRevision(_objId, revNb, _stateMap);
	}
	
	public static final boolean same(Map<String, Object> val, Map<String, Object> otherVal) {
		if (val == otherVal)
			return true;
		
		if ((val == null) && (otherVal != null) && (otherVal.isEmpty()))
			return true;
		
		if ((otherVal == null) && (val != null) && (val.isEmpty()))
			return true;
		
		if (val != null)
			return val.equals(otherVal);
		
		return false;
	}
}
