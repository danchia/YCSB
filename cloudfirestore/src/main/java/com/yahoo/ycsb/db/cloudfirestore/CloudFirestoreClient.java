/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.db.cloudfirestore;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.SetOptions;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * YCSB Client for Google's Cloud Firestore.
 */
public class CloudFirestoreClient extends DB {
  /**
   * The names of properties which can be specified in the config files and flags.
   */
  public static final class CloudFirestoreProperties {
    private CloudFirestoreProperties() {}

    /**
     * The Cloud Firestore project ID to use when running the YCSB benchmark.
     */
    static final String PROJECT = "cloudfirestore.project";
  }

  private static final Object LOCK_OBJECT = new Object();
  private static final Logger LOGGER = Logger.getLogger(CloudFirestoreClient.class.getName());

  private static Firestore db;

  @Override
  public void init() throws DBException {
    synchronized (LOCK_OBJECT) {
      if (db != null) {
        return;
      }
      Properties properties = getProperties();
      String projectId = properties.getProperty(CloudFirestoreProperties.PROJECT);
      if (projectId == null) {
        throw new DBException("Must provide project ID.");
      }

      FirestoreOptions options =
          FirestoreOptions.newBuilder().setProjectId(projectId).build();
      db = options.getService();

      LOGGER.info(String.format("Created Firestore client for project: %s", projectId));
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    DocumentReference reference = toReference(table, key);
    try {
      DocumentSnapshot document = reference.get().get();
      decode(fields, document, result);
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Interrupted during read().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.log(Level.WARNING, "Error during read().", e);
      return Status.ERROR;
    }
  }

  private void decode(
      Set<String> fields, DocumentSnapshot document, Map<String, ByteIterator> result) {
    Set<String> documentFields = fields == null ? document.getData().keySet() : fields;
    for (String field : documentFields) {
      String value = document.getString(field);
      result.put(field, new StringByteIterator(value));
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    Query query =
        db.collection(table)
            .orderBy("__name__")
            .startAt(startkey)
            .limit(recordcount);

    try {
      QuerySnapshot results = query.get().get();
      for (DocumentSnapshot document : results.getDocuments()) {
        HashMap<String, ByteIterator> d = new HashMap<>();
        decode(fields, document, d);
        result.add(d);
      }
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Interrupted during scan().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.log(Level.WARNING, "Error during scan().", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    DocumentReference reference = toReference(table, key);
    Map<String, Object> data = toData(values);

    try {
      reference.set(data, SetOptions.merge()).get();
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Interrupted during update().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.log(Level.WARNING, "Error during update().", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    DocumentReference reference = toReference(table, key);
    Map<String, Object> data = toData(values);

    try {
      reference.create(data).get();
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Interrupted during insert().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.log(Level.WARNING, "Error during insert().", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    DocumentReference reference = toReference(table, key);

    try {
      reference.delete().get();
      return Status.OK;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Interrupted during delete().", e);
      return Status.ERROR;
    } catch (ExecutionException e) {
      LOGGER.log(Level.WARNING, "Error during delete().", e);
      return Status.ERROR;
    }
  }

  private DocumentReference toReference(String table, String key) {
    return db.collection(table).document(key);
  }

  private Map<String, Object> toData(Map<String, ByteIterator> values) {
    Map<String, Object> data = new HashMap<>(values.size());
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      data.put(entry.getKey(), entry.getValue().toString());
    }
    return data;
  }
}
