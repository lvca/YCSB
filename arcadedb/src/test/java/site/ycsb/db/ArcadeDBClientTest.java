/**
 * Copyright (c) 2015 - 2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexCursor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.StringByteIterator;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by kruthar on 12/29/15.
 */
public class ArcadeDBClientTest {
  // TODO: This must be copied because it is private in ArcadeDBClient, but this should defer to table property.
  private static final String TYPE_NAME    = "usertable";
  private static final int    FIELD_LENGTH = 32;
  private static final String FIELD_PREFIX = "FIELD";
  private static final String KEY_PREFIX   = "user";
  private static final int    NUM_FIELDS   = 3;
  private static final String TEST_DB_URL  = "test";

  private static ArcadeDBClient arcadeDBClient = null;

  @Before
  public void setup() throws DBException {
    arcadeDBClient = new ArcadeDBClient();

    Properties p = new Properties();
    // TODO: Extract the property names into final variables in ArcadeDBClient
    p.setProperty("arcadedb.url", TEST_DB_URL);

    arcadeDBClient.setProperties(p);
    arcadeDBClient.init();
  }

  @After
  public void teardown() throws DBException {
    if (arcadeDBClient != null) {
      arcadeDBClient.cleanup();
    }
  }

  /*
      This is a copy of buildDeterministicValue() from core:site.ycsb.workloads.CoreWorkload.java.
      That method is neither public nor static so we need a copy.
   */
  private String buildDeterministicValue(final String key, final String fieldkey) {
    int size = FIELD_LENGTH;
    final StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  /*
      Inserts a row of deterministic values for the given insertKey using the ArcadeDBClient.
   */
  private Map<String, ByteIterator> insertRow(String insertKey) {
    final HashMap<String, ByteIterator> insertMap = new HashMap<>(3);
    for (int i = 0; i < 3; i++)
      insertMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(insertKey, FIELD_PREFIX + i)));

    arcadeDBClient.insert(TYPE_NAME, insertKey, insertMap);

    return insertMap;
  }

  @Test
  public void insertTest() {
    String insertKey = "user0";
    Map<String, ByteIterator> insertMap = insertRow(insertKey);

    Database database = arcadeDBClient.getDatabase();
    database.transaction(() -> {
      final IndexCursor cursor = database.lookupByKey(TYPE_NAME, "id", insertKey);
      assertTrue(cursor.hasNext());
      final Document result = cursor.next().asDocument(true);
      assertTrue("Assert a row was inserted.", result != null);

      for (int i = 0; i < NUM_FIELDS; i++)
        assertEquals("Assert all inserted columns have correct values.", result.get(FIELD_PREFIX + i), insertMap.get(FIELD_PREFIX + i).toString());
    });
  }

  @Test
  public void updateTest() {
    String preupdateString = "preupdate";
    String user0 = "user0";
    String user1 = "user1";
    String user2 = "user2";

    Database database = arcadeDBClient.getDatabase();
    database.transaction(() -> {
      // Manually insert three documents
      for (String key : Arrays.asList(user0, user1, user2)) {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        for (int i = 0; i < NUM_FIELDS; i++)
          doc.set(FIELD_PREFIX + i, preupdateString);

        doc.save();
      }
    });

    HashMap<String, ByteIterator> updateMap = new HashMap<>();
    for (int i = 0; i < NUM_FIELDS; i++)
      updateMap.put(FIELD_PREFIX + i, new StringByteIterator(buildDeterministicValue(user1, FIELD_PREFIX + i)));

    arcadeDBClient.update(TYPE_NAME, user1, updateMap);

    // Ensure that user0 record was not changed
    IndexCursor cursor = database.lookupByKey(TYPE_NAME, "id", user0);

    assertTrue(cursor.hasNext());
    final Document result = cursor.next().asDocument(true);

    for (int i = 0; i < NUM_FIELDS; i++)
      assertEquals("Assert first row fields contain preupdateString", result.get(FIELD_PREFIX + i), preupdateString);

    // Check that all the columns have expected values for user1 record
    cursor = database.lookupByKey(TYPE_NAME, "id", user1);
    assertTrue(cursor.hasNext());
    for (int i = 0; i < NUM_FIELDS; i++)
      assertEquals("Assert updated row fields are correct", result.get(FIELD_PREFIX + i), updateMap.get(FIELD_PREFIX + i).toString());

    // Ensure that user2 record was not changed
    cursor = database.lookupByKey(TYPE_NAME, "id", user2);
    for (int i = 0; i < NUM_FIELDS; i++)
      assertEquals("Assert third row fields contain preupdateString", result.get(FIELD_PREFIX + i), preupdateString);
  }

  @Test
  public void readTest() {
    String insertKey = "user0";
    Map<String, ByteIterator> insertMap = insertRow(insertKey);
    HashSet<String> readFields = new HashSet<>();
    HashMap<String, ByteIterator> readResultMap = new HashMap<>();

    // Test reading a single field
    readFields.add("FIELD0");
    arcadeDBClient.read(TYPE_NAME, insertKey, readFields, readResultMap);
    assertEquals("Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (String field : readFields) {
      assertEquals("Assert " + field + " was read correctly", insertMap.get(field).toString(), readResultMap.get(field).toString());
    }

    readResultMap = new HashMap<>();

    // Test reading all fields
    readFields.add("FIELD1");
    readFields.add("FIELD2");
    arcadeDBClient.read(TYPE_NAME, insertKey, readFields, readResultMap);
    assertEquals("Assert that result has correct number of fields", readFields.size(), readResultMap.size());
    for (String field : readFields) {
      assertEquals("Assert " + field + " was read correctly", insertMap.get(field).toString(), readResultMap.get(field).toString());
    }
  }

  @Test
  public void deleteTest() {
    String user0 = "user0";
    String user1 = "user1";
    String user2 = "user2";

    insertRow(user0);
    insertRow(user1);
    insertRow(user2);

    arcadeDBClient.delete(TYPE_NAME, user1);

    Database database = arcadeDBClient.getDatabase();

    assertTrue("Assert user0 still exists", database.lookupByKey(TYPE_NAME, "id", user0).hasNext());
    assertFalse("Assert user1 does not exist", database.lookupByKey(TYPE_NAME, "id", user1).hasNext());
    assertTrue("Assert user2 still exists", database.lookupByKey(TYPE_NAME, "id", user2).hasNext());
  }

  @Test
  public void scanTest() {
    Map<String, Map<String, ByteIterator>> keyMap = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String insertKey = KEY_PREFIX + i;
      keyMap.put(insertKey, insertRow(insertKey));
    }

    Set<String> fieldSet = new HashSet<>();
    fieldSet.add("FIELD0");
    fieldSet.add("FIELD1");
    int startIndex = 0;
    int resultRows = 3;

    Vector<HashMap<String, ByteIterator>> resultVector = new Vector<>();
    arcadeDBClient.scan(TYPE_NAME, KEY_PREFIX + startIndex, resultRows, fieldSet, resultVector);

    // Check the resultVector is the correct size
    assertEquals("Assert the correct number of results rows were returned", resultRows, resultVector.size());

    int testIndex = startIndex;

    // Check each vector row to make sure we have the correct fields
    for (HashMap<String, ByteIterator> result : resultVector) {
      assertEquals("Assert that this row has the correct number of fields", fieldSet.size(), result.size());
      for (String field : fieldSet) {
        assertEquals("Assert this field is correct in this row", keyMap.get(KEY_PREFIX + testIndex).get(field).toString(), result.get(field).toString());
      }
      testIndex++;
    }
  }
}
