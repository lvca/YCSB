/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * ArcadeDB client for YCSB framework.
 */
public class ArcadeDBClient extends DB {
  private static final String DATABASE_NAME          = "ycsb";
  private static final String DEFAULT_USER           = "root";
  private static final String DEFAULT_PASSWORD       = "root";
  private static final String URL_PROPERTY           = "arcadedb.url";
  private static final String URL_PROPERTY_DEFAULT   = "." + File.separator + "target" + File.separator + "databases" + File.separator + DATABASE_NAME;
  private static final String NEWDB_PROPERTY         = "arcadedb.newdb";
  private static final String NEWDB_PROPERTY_DEFAULT = "false";
  private static final String ARCADEDB_DOCUMENT_TYPE = "document";
  private static final String TYPE_NAME              = "usertable";

  private DatabaseFactory factory;
  private Database        database;
  private RemoteDatabase  remoteDatabase;
  private TypeIndex       index;

  private static final Lock    INIT_LOCK     = new ReentrantLock();
  private static       boolean dbChecked     = false;
  private static       boolean initialized   = false;
  private static       int     clientCounter = 0;
  private              boolean isRemote      = false;

  private static final Logger LOG      = LoggerFactory.getLogger(ArcadeDBClient.class);
  private              String user     = DEFAULT_USER;
  private              String password = DEFAULT_PASSWORD;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    // initialize ArcadeDB driver
    final Properties props = getProperties();
    String url = props.getProperty(URL_PROPERTY, URL_PROPERTY_DEFAULT);
    Boolean newdb = Boolean.parseBoolean(props.getProperty(NEWDB_PROPERTY, NEWDB_PROPERTY_DEFAULT));

    INIT_LOCK.lock();
    try {
      clientCounter++;
      if (!initialized) {
        GlobalConfiguration.dumpConfiguration(System.out);

        LOG.info("ArcadeDB loading database url = " + url);

        factory = new DatabaseFactory(url);

        if (url.startsWith("remote:"))
          isRemote = true;

        if (!dbChecked) {
          if (!isRemote) {
            if (newdb) {
              if (factory.exists()) {
                database = factory.open();
                LOG.info("ArcadeDB drop and recreate fresh db");
                database.drop();
              }

              database = factory.create();
            } else {
              if (!factory.exists()) {
                LOG.info("ArcadeDB database not found, creating fresh db");

                database = factory.create();
              }
            }
          } else {
            remoteDatabase = new RemoteDatabase(url, 2480, DATABASE_NAME, user, password);

            if (newdb) {
              if (remoteDatabase.exists()) {
                LOG.info("ArcadeDB drop and recreate fresh db");
                remoteDatabase.drop();
              }

              remoteDatabase.create();
            } else {
              if (!remoteDatabase.exists()) {
                LOG.info("ArcadeDB database not found, creating fresh db");
                remoteDatabase.create();
              }
            }

            remoteDatabase.close();
          }

          dbChecked = true;
        }

        if (!database.isOpen())
          database = factory.open();

        if (!database.getSchema().existsType(TYPE_NAME))
          database.getSchema().createDocumentType(TYPE_NAME);

        index = database.getSchema().getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "id");

        initialized = true;
      }
    } catch (Exception e) {
      LOG.error("Could not initialize ArcadeDB connection pool for Loader: " + e.toString());
      e.printStackTrace();
    } finally {
      INIT_LOCK.unlock();
    }

  }

  Database getDatabase() {
    return database;
  }

  @Override
  public void cleanup() throws DBException {
    INIT_LOCK.lock();
    try {
      clientCounter--;
      if (clientCounter == 0) {
        database.close();
        database = null;
        initialized = false;
      }
    } finally {
      INIT_LOCK.unlock();
    }

  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      database.transaction(() -> {
        final MutableDocument document = database.newDocument(TYPE_NAME);

        for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet())
          document.set(entry.getKey(), entry.getValue());

        document.save();
      });

      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(final String table, final String key) {
    while (true) {
      try {
        database.transaction(() -> {
          final IndexCursor records = database.lookupByKey(TYPE_NAME, "id", key);
          while (records.hasNext())
            records.next().getRecord().delete();
        });
        return Status.OK;

      } catch (ConcurrentModificationException cme) {
        // RETRY
        continue;

      } catch (Exception e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields, final Map<String, ByteIterator> result) {
    try {
      final IndexCursor records = database.lookupByKey(TYPE_NAME, "id", key);

      if (records.hasNext()) {
        final Document document = records.next().asDocument(true);
        if (document != null) {
          if (fields != null) {
            // COPY ONLY THE REQUESTED PROPERTIES
            for (String field : fields)
              result.put(field, new StringByteIterator((String) document.get(field)));
          } else {
            // COPY ALL THE PROPERTIES
            for (String field : document.getPropertyNames())
              result.put(field, new StringByteIterator((String) document.get(field)));
          }
          return Status.OK;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    final AtomicReference<Status> status = new AtomicReference<>(Status.ERROR);

    while (true) {
      try {
        database.transaction(() -> {
          final IndexCursor records = database.lookupByKey(TYPE_NAME, "id", key);
          if (records.hasNext()) {
            final MutableDocument document = records.next().asDocument(true).modify();
            if (document != null) {
              for (Map.Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet())
                document.set(entry.getKey(), entry.getValue());

              document.save();
              status.set(Status.OK);
            }
          }
        });

        break;

      } catch (ConcurrentModificationException cme) {
        // RETRY
        continue;
      } catch (Exception e) {
        e.printStackTrace();
        break;
      }
    }

    return status.get();
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
      final Vector<HashMap<String, ByteIterator>> result) {

    if (isRemote) {
      // Iterator methods needed for scanning are Unsupported for remote database connections.
      LOG.warn("ArcadeDB scan operation is not implemented for remote database connections.");
      return Status.NOT_IMPLEMENTED;
    }

    try {
      final IndexCursor entries = index.range(true, new String[] { startkey }, true, null, true);

      int currentCount = 0;
      while (entries.hasNext()) {
        final Document document = entries.next().asDocument(true);

        final HashMap<String, ByteIterator> map;

        if (fields != null) {
          map = new HashMap<>(fields.size());
          for (String field : fields)
            map.put(field, new StringByteIterator((String) document.get(field)));

        } else {
          final Set<String> properties = document.getPropertyNames();
          map = new HashMap<>(properties.size());
          for (String field : properties) {
            map.put(field, new StringByteIterator((String) document.get(field)));
          }
        }

        result.add(map);

        currentCount++;

        if (currentCount >= recordcount) {
          break;
        }
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }
}
