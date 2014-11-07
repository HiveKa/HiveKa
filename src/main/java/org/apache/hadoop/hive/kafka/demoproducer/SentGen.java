/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.kafka.demoproducer;

import java.util.Random;

public class SentGen {

  /**
   * Public constructors
   */
  SentGen(){
    r = new Random();
  }

  SentGen(long seed){
    r = new Random( seed );
  }

  /**
   * Constants
   */
  final int NO_WORDS = 5;              // Number of each type word
  final String SPACE = " ";
  final String PERIOD = ".";
  final String article[] = { "the", "a", "that" };
  final String noun[] = { "boy", "girl", "dog", "manager","friend" };
  final String verb[] = { "drove", "jumped", "ran", "walked", "skipped" };
  final String preposition[] = { "to", "from", "over", "under", "on" };
  final String noun2[] = {"town","bridge","car","place"};



  /**
   *  Protected instance variable
   */
  Random r;			// For random numbers

  /**
   * Class methods
   */

  public String nextSent(){
    String sent = article[rand()%article.length];	// Initialize next random sentence
    char c = sent.charAt(0);	// Get first char of article &
    sent = sent.replace( c,Character.toUpperCase(c) );  // capitalize

    sent += SPACE + noun[rand()%noun.length] + SPACE + verb[rand()%verb.length] + SPACE +
        preposition[rand()%preposition.length] + SPACE + article[rand()%article.length] + SPACE +
        noun2[rand()%noun2.length] + PERIOD;

    return sent;

  }

  int rand(){
    int ri = r.nextInt() % NO_WORDS;
    if ( ri < 0 )
      ri += NO_WORDS;
    return ri;
  }
}