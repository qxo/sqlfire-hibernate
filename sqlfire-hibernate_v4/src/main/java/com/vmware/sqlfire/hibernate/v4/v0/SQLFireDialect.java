/* SQLFireDialect for Hibernate. 
 * 
 * Copyright (c) 2009-2012 VMware, Inc. All rights reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws.
 * 
 * This library (sqlfHibernateDialect.jar) is free software; you can 
 * redistribute it and/or modify it under the terms of the GNU Lesser 
 * General Public License as published by the Free Software Foundation; 
 * either version 2.1 of the License, or (at your option) any later 
 * version.
 * 
 * This library (sqlfHibernateDialect.jar) is distributed in the hope that
 * it will be useful, but WITHOUT ANY WARRANTY; without even the implied 
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
 * the GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  
 * 02110-1301  USA
 * 
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * 
 * Derived from sources of Hibernate DerbyDialect.
 */
package com.vmware.sqlfire.hibernate.v4.v0;

import java.sql.SQLException;

import org.hibernate.JDBCException;
import org.hibernate.dialect.function.AnsiTrimFunction;
import org.hibernate.dialect.function.DerbyConcatFunction;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.NvlFunction;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.exception.DataException;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.SQLGrammarException;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.internal.util.JdbcExceptionHelper;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.exception.spi.SQLExceptionConverter;

/*
 * Hibernate V4
 */
public class SQLFireDialect extends com.vmware.sqlfire.hibernate.SQLFireDialectBase {
	public SQLFireDialect() {
		super();
	        LOG.info("SQLFireDialect for Hibernate 4.0 initialized.");
	              
		registerFunction("concat", new DerbyConcatFunction());
		registerFunction("trim", new AnsiTrimFunction());
		registerFunction("value", new StandardSQLFunction("coalesce"));
		registerFunction("nvl", new NvlFunction());
		registerFunction("groups", new StandardSQLFunction("GROUPS",
				StandardBasicTypes.STRING));
		registerFunction("dsid", new StandardSQLFunction("DSID",
				StandardBasicTypes.STRING));
		registerFunction("groupsintersection", new StandardSQLFunction(
				"GROUPSINTERSECTION", StandardBasicTypes.STRING));
		registerFunction("groupsintersect", new StandardSQLFunction(
				"GROUPSINTERSECT", StandardBasicTypes.BOOLEAN));
		registerFunction("groupsunion", new StandardSQLFunction("GROUPSUNION",
				StandardBasicTypes.STRING));
		registerFunction("longint", new StandardSQLFunction("bigint",
				StandardBasicTypes.LONG));
		registerFunction("int", new StandardSQLFunction("integer",
				StandardBasicTypes.INTEGER));
		registerFunction("pi", new StandardSQLFunction("pi",
				StandardBasicTypes.DOUBLE));
		registerFunction("random", new NoArgSQLFunction("random",
				StandardBasicTypes.DOUBLE));
		registerFunction("rand", new StandardSQLFunction("rand",
				StandardBasicTypes.DOUBLE));// override
		registerFunction("sinh", new StandardSQLFunction("sinh",
				StandardBasicTypes.DOUBLE));
		registerFunction("cosh", new StandardSQLFunction("cosh",
				StandardBasicTypes.DOUBLE));
		registerFunction("tanh", new StandardSQLFunction("tanh",
				StandardBasicTypes.DOUBLE));
		registerFunction("user", new NoArgSQLFunction("USER",
				StandardBasicTypes.STRING, false));
		registerFunction("current_user", new NoArgSQLFunction("CURRENT_USER",
				StandardBasicTypes.STRING, false));
		registerFunction("session_user", new NoArgSQLFunction("SESSION_USER",
				StandardBasicTypes.STRING, false));
		registerFunction("current isolation", new NoArgSQLFunction(
				"CURRENT ISOLATION", StandardBasicTypes.STRING, false));
		registerFunction("current_role", new NoArgSQLFunction("CURRENT_ROLE",
				StandardBasicTypes.STRING, false));
		registerFunction("current schema", new NoArgSQLFunction(
				"CURRENT SCHEMA", StandardBasicTypes.STRING, false));
		registerFunction("current sqlid", new NoArgSQLFunction("CURRENT SQLID",
				StandardBasicTypes.STRING, false));
		registerFunction("xmlexists", new StandardSQLFunction("XMLEXISTS",
				StandardBasicTypes.NUMERIC_BOOLEAN));
		registerFunction("xmlparse", new StandardSQLFunction("XMLPARSE",
				StandardBasicTypes.TEXT));
		registerFunction("xmlquery", new StandardSQLFunction("XMLQUERY",
				StandardBasicTypes.STRING));
		registerFunction("xmlserialize", new StandardSQLFunction(
				"XMLSERIALIZE", StandardBasicTypes.STRING));
		registerFunction("get_current_connection", new NoArgSQLFunction(
				"GET_CURRENT_CONNECTION", StandardBasicTypes.BINARY, true));
		registerFunction("identity_val_local", new NoArgSQLFunction(
				"IDENTITY_VAL_LOCAL", StandardBasicTypes.BINARY, true));
	}

	@Override
	public boolean supportsPooledSequences() {
		return false;
	}

	@Override
	public boolean supportsUnboundedLobLocatorMaterialization() {
		return false;
	}
	
	@Override
	public boolean doesReadCommittedCauseWritersToBlockReaders() {
		return false;
	}
	
	/*
	 *  Override
	 */
	public String getWriteLockString(int timeout) {
		return " for update";
	}

	/*
	 *  Override
	 */
	public String getReadLockString(int timeout) {
		return " for update";
	}
	
        @Override
        public SQLExceptionConverter buildSQLExceptionConverter() {
                return new SQLExceptionConverter() {
                        @Override
                        public JDBCException convert(SQLException sqlException,
                                        String message, String sql) {
                                final String sqlState = JdbcExceptionHelper
                                                .extractSqlState(sqlException);
                                if (sqlState != null) {
                                        if (SQL_GRAMMAR_CATEGORIES.contains(sqlState)) {
                                                return new SQLGrammarException(message, sqlException,
                                                                sql);
                                        } else if (DATA_CATEGORIES.contains(sqlState)) {
                                                return new DataException(message, sqlException, sql);
                                        } else if (LOCK_ACQUISITION_CATEGORIES.contains(sqlState)) {
                                                return new LockAcquisitionException(message,
                                                                sqlException, sql);
                                        }
                                }
                                return null;
                        }
                };
        }
}
