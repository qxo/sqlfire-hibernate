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

package com.vmware.sqlfire.hibernate;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import org.hibernate.MappingException;
import org.hibernate.dialect.DB2Dialect;
import org.hibernate.sql.CaseFragment;
import org.hibernate.sql.DerbyCaseFragment;

/* 
 * SQLFireDialectBase
 */
public abstract class SQLFireDialectBase extends DB2Dialect {
  protected Logger LOG = Logger.getLogger("SQLFireDialect");
  protected boolean identityColumnStringIsAlways = true;

  /*
   * SQLFireDialectBase Constructor
   */
  protected SQLFireDialectBase() {
    super();
    String identityColumnStr = System.getenv("SQLF_GENERATE_IDENTITY");
    if (identityColumnStr != null
        && identityColumnStr.equalsIgnoreCase("BYDEFAULT")) {
      identityColumnStringIsAlways = false;
    }
    // Define nothing else here
  }

  // as per Derby
  @Override
  public CaseFragment createCaseFragment() {
    return new DerbyCaseFragment();
  }

  @Override
  public boolean dropConstraints() {
    return true;
  }

  @Override
  public boolean supportsSequences() {
    return false;
  }

  @Override
  public String getSequenceNextValString(String sequenceName)
      throws MappingException {
    throw new MappingException("Sqlfire Dialect does not support sequences");
  }

  @Override
  public boolean supportsCommentOn() {
    return false;
  }

  @Override
  public String getForUpdateString() {
    return " for update";
  }

  @Override
  public String getLimitString(String query, final int offset, final int limit) {
    // Get position of With
    int withPosition = query.lastIndexOf("with ");
    if (withPosition < 0) {
      withPosition = query.lastIndexOf("WITH ");
    }

    // Get position of for Update
    final String normalizedSelect = query.toLowerCase().trim();
    final int forUpdatePosition = normalizedSelect.lastIndexOf("for update");

    // Spring builder can expand...
    StringBuilder sb = new StringBuilder(query.length() + 50);
    if (forUpdatePosition >= 0) {
      sb.append(query.substring(0, forUpdatePosition - 1));
    }
    else if (normalizedSelect
        .startsWith("with ", normalizedSelect.length() - 7)) {
      sb.append(query.substring(0, withPosition - 1));
    }
    else {
      sb.append(query);
    }

    if (offset == 0) {
      sb.append(" fetch first ");
    }
    else {
      sb.append(" offset ").append(offset).append(" rows fetch next ");
    }

    sb.append(limit).append(" rows only");

    if (forUpdatePosition >= 0) {
      sb.append(' ');
      sb.append(query.substring(forUpdatePosition));
    }
    else if (normalizedSelect
        .startsWith("with ", normalizedSelect.length() - 7)) {
      sb.append(' ').append(query.substring(withPosition));
    }
    return sb.toString();
  }

  @Override
  public String getQuerySequencesString() {
    return null;
  }

  @Override
  public String getTableComment(String comment) {
    return comment;
  }

  @Override
  public boolean useMaxForLimit() {
    return false;
  }

  @Override
  public boolean supportsIfExistsBeforeTableName() {
    return true;
  }

  @Override
  public String getIdentityColumnString() {
    if (identityColumnStringIsAlways) {
      return "generated always as identity";
    }
    return "generated by default as identity";
  }
  
  /*
   * For purpose of buildSQLExceptionConversionDelegate in V4 or
   * buildSQLExceptionConverter in V3
   */
  protected static final Set<String> SQL_GRAMMAR_CATEGORIES = buildGrammarCategories();

  protected static Set<String> buildGrammarCategories() {
    HashSet<String> categories = new HashSet<String>();
    categories.addAll(Arrays.asList("X0Y91", // LANG_INVALID_COLOCATION_DIFFERENT_COL_COUNT
        "X0Y92", // LANG_INVALID_COLOCATION_COL_TYPES_MISMATCH
        "X0Y93", // LANG_INVALID_COLOCATION_DIFFERENT_GROUPS
        "X0Y94", // LANG_INVALID_COLOCATION_REDUNDANCY_OR_BUCKETS_MISMATCH
        "X0Y95", // LANG_INVALID_COLOCATION_PARTITIONINGPOLICY_MISMATCH
        "X0Y96", // LANG_INVALID_COLOCATION
        "X0Y97", // LANG_INVALID_PARTITIONING_NO_PK
        "X0Y99", // LANG_INVALID_REFERENCE
        "X0Z11", // AUTOINCREMENT_GIVEN_START_NOT_SUPPORTED
        "X0Z12", // AUTOINCREMENT_GIVEN_INCREMENT_NOT_SUPPORTED
        "X0Z13", // GENERATED_KEY_OVERFLOW
        "X0Z15", // TABLE_NOT_PARTITIONED
        "X0Z17" // DAP_RESULT_PROCESSOR_INTERFACE_MISSING
    ));
    return Collections.unmodifiableSet(categories);
  }

  protected static final Set<String> DATA_CATEGORIES = buildDataCategories();

  protected static Set<String> buildDataCategories() {
    HashSet<String> categories = new HashSet<String>();
    categories.addAll(Arrays.asList("X0Y98", // COLOCATED_CHILDREN_EXIST
        "X0Z01", // SQLF_NODE_SHUTDOWN_PREFIX
        "X0Z04", // LANG_INVALID_MEMBER_REFERENCE
        "X0Z05", // SQLF_DATA_NOT_COLOCATED_PREFIX
        "X0Z08", // NO_DATASTORE_FOUND
        "X0Z09", // INSUFFICIENT_DATASTORE
        "X0Z18" // SQLF_NODE_BUCKET_MOVED_PREFIX
    ));
    return Collections.unmodifiableSet(categories);
  }

  protected static final Set<String> LOCK_ACQUISITION_CATEGORIES = buildLockAcquisitionCategories();

  protected static Set<String> buildLockAcquisitionCategories() {
    HashSet<String> categories = new HashSet<String>();
    categories.add("X0Z02" // SQLF_OPERATION_CONFLICT_PREFIX
        );
    return Collections.unmodifiableSet(categories);
  }

}