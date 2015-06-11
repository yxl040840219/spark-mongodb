/*
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.deep.mongodb.writer

import com.mongodb.MongoCredential
import com.mongodb.casbah.Imports._
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig

/**
 * Abstract Mongodb writer.
 * Used for saving a bunch of mongodb objects
 * into specified database and collection
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
abstract class MongodbWriter(config: DeepConfig) extends Serializable {

  /**
   * A MongoDB client is created for each writer.
   */
  protected val mongoClient: MongoClient =
    MongoClient(
      config[List[String]](MongodbConfig.Host)
        .map(add => new ServerAddress(add)),
      config[List[MongoCredential]](MongodbConfig.Credentials))

  /**
   * A MongoDB collection created from the specified database and collection.
   */
  protected val dbCollection: MongoCollection =
    mongoClient(config(MongodbConfig.Database))(config(MongodbConfig.Collection))

  /**
   * Abstract method that checks if a primary key exists in provided configuration
   * and calls the 'save' method afterwards.
   *
   * @param it DBObject iterator.
   */
  def saveWithPk (it: Iterator[DBObject]): Unit = {
    config.get[String](MongodbConfig.PrimaryKey).fold(
      save(it)) { pk =>
      save(it.map {
        case obj: BasicDBObject =>
          obj.append("_id", obj.get(pk)) // 插入 _id
      })
    }
  }

  /**
   * Abstract method for storing a bunch of MongoDB objects.
   *
   * @param it Iterator of mongodb objects.
   */
  def save(it: Iterator[DBObject]): Unit

  /**
   * Close current MongoDB client.
   */
  def close(): Unit = {
    mongoClient.close()
  }

}