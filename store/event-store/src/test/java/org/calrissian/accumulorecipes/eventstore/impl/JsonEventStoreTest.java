/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.eventstore.impl;

import java.util.Date;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.calrissian.accumulorecipes.commons.domain.Auths;
import org.calrissian.accumulorecipes.eventstore.EventStore;
import org.calrissian.accumulorecipes.eventstore.support.shard.HourlyShardBuilder;
import org.calrissian.accumulorecipes.test.AccumuloTestUtils;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.criteria.builder.QueryBuilder;
import org.calrissian.mango.criteria.domain.Node;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.calrissian.mango.json.util.json.JsonTupleStore;
import org.junit.Before;
import org.junit.Test;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_IDX_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_SHARD_TABLE_NAME;
import static org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore.DEFAULT_STORE_CONFIG;
import static org.calrissian.mango.types.LexiTypeEncoders.LEXI_TYPES;
import static org.junit.Assert.assertEquals;

public class JsonEventStoreTest {

  private Connector connector;
  private EventStore store;
  private ObjectMapper objectMapper = new ObjectMapper();

  public static Connector getConnector() throws AccumuloSecurityException, AccumuloException {
    return new MockInstance().getConnector("root", "".getBytes());
  }

  @Before
  public void setup() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    connector = getConnector();
    store = new AccumuloEventStore(connector, DEFAULT_IDX_TABLE_NAME, DEFAULT_SHARD_TABLE_NAME, DEFAULT_STORE_CONFIG, LEXI_TYPES, new HourlyShardBuilder(50));
  }

  @Test
  public void testTwitterJson() throws Exception {
    String json = "{\n"
        + "  \"statuses\": [\n"
        + "    {\n"
        + "      \"coordinates\": null,\n"
        + "      \"favorited\": false,\n"
        + "      \"truncated\": false,\n"
        + "      \"created_at\": \"Mon Sep 24 03:35:21 +0000 2012\",\n"
        + "      \"id_str\": \"250075927172759552\",\n"
        + "      \"entities\": {\n"
        + "        \"urls\": [\n"
        + " \n"
        + "        ],\n"
        + "        \"hashtags\": [\n"
        + "          {\n"
        + "            \"text\": \"freebandnames\",\n"
        + "            \"indices\": [\n"
        + "              20,\n"
        + "              34\n"
        + "            ]\n"
        + "          }\n"
        + "        ],\n"
        + "        \"user_mentions\": [\n"
        + " \n"
        + "        ]\n"
        + "      },\n"
        + "      \"in_reply_to_user_id_str\": null,\n"
        + "      \"contributors\": null,\n"
        + "      \"text\": \"Aggressive Ponytail #freebandnames\",\n"
        + "      \"metadata\": {\n"
        + "        \"iso_language_code\": \"en\",\n"
        + "        \"result_type\": \"recent\"\n"
        + "      },\n"
        + "      \"retweet_count\": 0,\n"
        + "      \"in_reply_to_status_id_str\": null,\n"
        + "      \"id\": 250075927172759552,\n"
        + "      \"geo\": null,\n"
        + "      \"retweeted\": false,\n"
        + "      \"in_reply_to_user_id\": null,\n"
        + "      \"place\": null,\n"
        + "      \"user\": {\n"
        + "        \"profile_sidebar_fill_color\": \"DDEEF6\",\n"
        + "        \"profile_sidebar_border_color\": \"C0DEED\",\n"
        + "        \"profile_background_tile\": false,\n"
        + "        \"name\": \"Sean Cummings\",\n"
        + "        \"profile_image_url\": \"http://a0.twimg.com/profile_images/2359746665/1v6zfgqo8g0d3mk7ii5s_normal.jpeg\",\n"
        + "        \"created_at\": \"Mon Apr 26 06:01:55 +0000 2010\",\n"
        + "        \"location\": \"LA, CA\",\n"
        + "        \"follow_request_sent\": null,\n"
        + "        \"profile_link_color\": \"0084B4\",\n"
        + "        \"is_translator\": false,\n"
        + "        \"id_str\": \"137238150\",\n"
        + "        \"entities\": {\n"
        + "          \"url\": {\n"
        + "            \"urls\": [\n"
        + "              {\n"
        + "                \"expanded_url\": null,\n"
        + "                \"url\": \"\",\n"
        + "                \"indices\": [\n"
        + "                  0,\n"
        + "                  0\n"
        + "                ]\n"
        + "              }\n"
        + "            ]\n"
        + "          },\n"
        + "          \"description\": {\n"
        + "            \"urls\": [\n"
        + " \n"
        + "            ]\n"
        + "          }\n"
        + "        },\n"
        + "        \"default_profile\": true,\n"
        + "        \"contributors_enabled\": false,\n"
        + "        \"favourites_count\": 0,\n"
        + "        \"url\": null,\n"
        + "        \"profile_image_url_https\": \"https://si0.twimg.com/profile_images/2359746665/1v6zfgqo8g0d3mk7ii5s_normal.jpeg\",\n"
        + "        \"utc_offset\": -28800,\n"
        + "        \"id\": 137238150,\n"
        + "        \"profile_use_background_image\": true,\n"
        + "        \"listed_count\": 2,\n"
        + "        \"profile_text_color\": \"333333\",\n"
        + "        \"lang\": \"en\",\n"
        + "        \"followers_count\": 70,\n"
        + "        \"protected\": false,\n"
        + "        \"notifications\": null,\n"
        + "        \"profile_background_image_url_https\": \"https://si0.twimg.com/images/themes/theme1/bg.png\",\n"
        + "        \"profile_background_color\": \"C0DEED\",\n"
        + "        \"verified\": false,\n"
        + "        \"geo_enabled\": true,\n"
        + "        \"time_zone\": \"Pacific Time (US & Canada)\",\n"
        + "        \"description\": \"Born 330 Live 310\",\n"
        + "        \"default_profile_image\": false,\n"
        + "        \"profile_background_image_url\": \"http://a0.twimg.com/images/themes/theme1/bg.png\",\n"
        + "        \"statuses_count\": 579,\n"
        + "        \"friends_count\": 110,\n"
        + "        \"following\": null,\n"
        + "        \"show_all_inline_media\": false,\n"
        + "        \"screen_name\": \"sean_cummings\"\n"
        + "      },\n"
        + "      \"in_reply_to_screen_name\": null,\n"
        + "      \"source\": \"<a>Twitter for Mac</a>\",\n"
        + "      \"in_reply_to_status_id\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"coordinates\": null,\n"
        + "      \"favorited\": false,\n"
        + "      \"truncated\": false,\n"
        + "      \"created_at\": \"Fri Sep 21 23:40:54 +0000 2012\",\n"
        + "      \"id_str\": \"249292149810667520\",\n"
        + "      \"entities\": {\n"
        + "        \"urls\": [\n"
        + " \n"
        + "        ],\n"
        + "        \"hashtags\": [\n"
        + "          {\n"
        + "            \"text\": \"FreeBandNames\",\n"
        + "            \"indices\": [\n"
        + "              20,\n"
        + "              34\n"
        + "            ]\n"
        + "          }\n"
        + "        ],\n"
        + "        \"user_mentions\": [\n"
        + " \n"
        + "        ]\n"
        + "      },\n"
        + "      \"in_reply_to_user_id_str\": null,\n"
        + "      \"contributors\": null,\n"
        + "      \"text\": \"Thee Namaste Nerdz. #FreeBandNames\",\n"
        + "      \"metadata\": {\n"
        + "        \"iso_language_code\": \"pl\",\n"
        + "        \"result_type\": \"recent\"\n"
        + "      },\n"
        + "      \"retweet_count\": 0,\n"
        + "      \"in_reply_to_status_id_str\": null,\n"
        + "      \"id\": 249292149810667520,\n"
        + "      \"geo\": null,\n"
        + "      \"retweeted\": false,\n"
        + "      \"in_reply_to_user_id\": null,\n"
        + "      \"place\": null,\n"
        + "      \"user\": {\n"
        + "        \"profile_sidebar_fill_color\": \"DDFFCC\",\n"
        + "        \"profile_sidebar_border_color\": \"BDDCAD\",\n"
        + "        \"profile_background_tile\": true,\n"
        + "        \"name\": \"Chaz Martenstein\",\n"
        + "        \"profile_image_url\": \"http://a0.twimg.com/profile_images/447958234/Lichtenstein_normal.jpg\",\n"
        + "        \"created_at\": \"Tue Apr 07 19:05:07 +0000 2009\",\n"
        + "        \"location\": \"Durham, NC\",\n"
        + "        \"follow_request_sent\": null,\n"
        + "        \"profile_link_color\": \"0084B4\",\n"
        + "        \"is_translator\": false,\n"
        + "        \"id_str\": \"29516238\",\n"
        + "        \"entities\": {\n"
        + "          \"url\": {\n"
        + "            \"urls\": [\n"
        + "              {\n"
        + "                \"expanded_url\": null,\n"
        + "                \"url\": \"http://bullcityrecords.com/wnng/\",\n"
        + "                \"indices\": [\n"
        + "                  0,\n"
        + "                  32\n"
        + "                ]\n"
        + "              }\n"
        + "            ]\n"
        + "          },\n"
        + "          \"description\": {\n"
        + "            \"urls\": [\n"
        + " \n"
        + "            ]\n"
        + "          }\n"
        + "        },\n"
        + "        \"default_profile\": false,\n"
        + "        \"contributors_enabled\": false,\n"
        + "        \"favourites_count\": 8,\n"
        + "        \"url\": \"http://bullcityrecords.com/wnng/\",\n"
        + "        \"profile_image_url_https\": \"https://si0.twimg.com/profile_images/447958234/Lichtenstein_normal.jpg\",\n"
        + "        \"utc_offset\": -18000,\n"
        + "        \"id\": 29516238,\n"
        + "        \"profile_use_background_image\": true,\n"
        + "        \"listed_count\": 118,\n"
        + "        \"profile_text_color\": \"333333\",\n"
        + "        \"lang\": \"en\",\n"
        + "        \"followers_count\": 2052,\n"
        + "        \"protected\": false,\n"
        + "        \"notifications\": null,\n"
        + "        \"profile_background_image_url_https\": \"https://si0.twimg.com/profile_background_images/9423277/background_tile.bmp\",\n"
        + "        \"profile_background_color\": \"9AE4E8\",\n"
        + "        \"verified\": false,\n"
        + "        \"geo_enabled\": false,\n"
        + "        \"time_zone\": \"Eastern Time (US & Canada)\",\n"
        + "        \"description\": \"You will come to Durham, North Carolina. I will sell you some records then, here in Durham, North Carolina. Fun will happen.\",\n"
        + "        \"default_profile_image\": false,\n"
        + "        \"profile_background_image_url\": \"http://a0.twimg.com/profile_background_images/9423277/background_tile.bmp\",\n"
        + "        \"statuses_count\": 7579,\n"
        + "        \"friends_count\": 348,\n"
        + "        \"following\": null,\n"
        + "        \"show_all_inline_media\": true,\n"
        + "        \"screen_name\": \"bullcityrecords\"\n"
        + "      },\n"
        + "      \"in_reply_to_screen_name\": null,\n"
        + "      \"source\": \"web\",\n"
        + "      \"in_reply_to_status_id\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"coordinates\": null,\n"
        + "      \"favorited\": false,\n"
        + "      \"truncated\": false,\n"
        + "      \"created_at\": \"Fri Sep 21 23:30:20 +0000 2012\",\n"
        + "      \"id_str\": \"249289491129438208\",\n"
        + "      \"entities\": {\n"
        + "        \"urls\": [\n"
        + " \n"
        + "        ],\n"
        + "        \"hashtags\": [\n"
        + "          {\n"
        + "            \"text\": \"freebandnames\",\n"
        + "            \"indices\": [\n"
        + "              29,\n"
        + "              43\n"
        + "            ]\n"
        + "          }\n"
        + "        ],\n"
        + "        \"user_mentions\": [\n"
        + " \n"
        + "        ]\n"
        + "      },\n"
        + "      \"in_reply_to_user_id_str\": null,\n"
        + "      \"contributors\": null,\n"
        + "      \"text\": \"Mexican Heaven, Mexican Hell #freebandnames\",\n"
        + "      \"metadata\": {\n"
        + "        \"iso_language_code\": \"en\",\n"
        + "        \"result_type\": \"recent\"\n"
        + "      },\n"
        + "      \"retweet_count\": 0,\n"
        + "      \"in_reply_to_status_id_str\": null,\n"
        + "      \"id\": 249289491129438208,\n"
        + "      \"geo\": null,\n"
        + "      \"retweeted\": false,\n"
        + "      \"in_reply_to_user_id\": null,\n"
        + "      \"place\": null,\n"
        + "      \"user\": {\n"
        + "        \"profile_sidebar_fill_color\": \"99CC33\",\n"
        + "        \"profile_sidebar_border_color\": \"829D5E\",\n"
        + "        \"profile_background_tile\": false,\n"
        + "        \"name\": \"Thomas John Wakeman\",\n"
        + "        \"profile_image_url\": \"http://a0.twimg.com/profile_images/2219333930/Froggystyle_normal.png\",\n"
        + "        \"created_at\": \"Tue Sep 01 21:21:35 +0000 2009\",\n"
        + "        \"location\": \"Kingston New York\",\n"
        + "        \"follow_request_sent\": null,\n"
        + "        \"profile_link_color\": \"D02B55\",\n"
        + "        \"is_translator\": false,\n"
        + "        \"id_str\": \"70789458\",\n"
        + "        \"entities\": {\n"
        + "          \"url\": {\n"
        + "            \"urls\": [\n"
        + "              {\n"
        + "                \"expanded_url\": null,\n"
        + "                \"url\": \"\",\n"
        + "                \"indices\": [\n"
        + "                  0,\n"
        + "                  0\n"
        + "                ]\n"
        + "              }\n"
        + "            ]\n"
        + "          },\n"
        + "          \"description\": {\n"
        + "            \"urls\": [\n"
        + " \n"
        + "            ]\n"
        + "          }\n"
        + "        },\n"
        + "        \"default_profile\": false,\n"
        + "        \"contributors_enabled\": false,\n"
        + "        \"favourites_count\": 19,\n"
        + "        \"url\": null,\n"
        + "        \"profile_image_url_https\": \"https://si0.twimg.com/profile_images/2219333930/Froggystyle_normal.png\",\n"
        + "        \"utc_offset\": -18000,\n"
        + "        \"id\": 70789458,\n"
        + "        \"profile_use_background_image\": true,\n"
        + "        \"listed_count\": 1,\n"
        + "        \"profile_text_color\": \"3E4415\",\n"
        + "        \"lang\": \"en\",\n"
        + "        \"followers_count\": 63,\n"
        + "        \"protected\": false,\n"
        + "        \"notifications\": null,\n"
        + "        \"profile_background_image_url_https\": \"https://si0.twimg.com/images/themes/theme5/bg.gif\",\n"
        + "        \"profile_background_color\": \"352726\",\n"
        + "        \"verified\": false,\n"
        + "        \"geo_enabled\": false,\n"
        + "        \"time_zone\": \"Eastern Time (US & Canada)\",\n"
        + "        \"description\": \"Science Fiction Writer, sort of. Likes Superheroes, Mole People, Alt. Timelines.\",\n"
        + "        \"default_profile_image\": false,\n"
        + "        \"profile_background_image_url\": \"http://a0.twimg.com/images/themes/theme5/bg.gif\",\n"
        + "        \"statuses_count\": 1048,\n"
        + "        \"friends_count\": 63,\n"
        + "        \"following\": null,\n"
        + "        \"show_all_inline_media\": false,\n"
        + "        \"screen_name\": \"MonkiesFist\"\n"
        + "      },\n"
        + "      \"in_reply_to_screen_name\": null,\n"
        + "      \"source\": \"web\",\n"
        + "      \"in_reply_to_status_id\": null\n"
        + "    },\n"
        + "    {\n"
        + "      \"coordinates\": null,\n"
        + "      \"favorited\": false,\n"
        + "      \"truncated\": false,\n"
        + "      \"created_at\": \"Fri Sep 21 22:51:18 +0000 2012\",\n"
        + "      \"id_str\": \"249279667666817024\",\n"
        + "      \"entities\": {\n"
        + "        \"urls\": [\n"
        + " \n"
        + "        ],\n"
        + "        \"hashtags\": [\n"
        + "          {\n"
        + "            \"text\": \"freebandnames\",\n"
        + "            \"indices\": [\n"
        + "              20,\n"
        + "              34\n"
        + "            ]\n"
        + "          }\n"
        + "        ],\n"
        + "        \"user_mentions\": [\n"
        + " \n"
        + "        ]\n"
        + "      },\n"
        + "      \"in_reply_to_user_id_str\": null,\n"
        + "      \"contributors\": null,\n"
        + "      \"text\": \"The Foolish Mortals #freebandnames\",\n"
        + "      \"metadata\": {\n"
        + "        \"iso_language_code\": \"en\",\n"
        + "        \"result_type\": \"recent\"\n"
        + "      },\n"
        + "      \"retweet_count\": 0,\n"
        + "      \"in_reply_to_status_id_str\": null,\n"
        + "      \"id\": 249279667666817024,\n"
        + "      \"geo\": null,\n"
        + "      \"retweeted\": false,\n"
        + "      \"in_reply_to_user_id\": null,\n"
        + "      \"place\": null,\n"
        + "      \"user\": {\n"
        + "        \"profile_sidebar_fill_color\": \"BFAC83\",\n"
        + "        \"profile_sidebar_border_color\": \"615A44\",\n"
        + "        \"profile_background_tile\": true,\n"
        + "        \"name\": \"Marty Elmer\",\n"
        + "        \"profile_image_url\": \"http://a0.twimg.com/profile_images/1629790393/shrinker_2000_trans_normal.png\",\n"
        + "        \"created_at\": \"Mon May 04 00:05:00 +0000 2009\",\n"
        + "        \"location\": \"Wisconsin, USA\",\n"
        + "        \"follow_request_sent\": null,\n"
        + "        \"profile_link_color\": \"3B2A26\",\n"
        + "        \"is_translator\": false,\n"
        + "        \"id_str\": \"37539828\",\n"
        + "        \"entities\": {\n"
        + "          \"url\": {\n"
        + "            \"urls\": [\n"
        + "              {\n"
        + "                \"expanded_url\": null,\n"
        + "                \"url\": \"http://www.omnitarian.me\",\n"
        + "                \"indices\": [\n"
        + "                  0,\n"
        + "                  24\n"
        + "                ]\n"
        + "              }\n"
        + "            ]\n"
        + "          },\n"
        + "          \"description\": {\n"
        + "            \"urls\": [\n"
        + " \n"
        + "            ]\n"
        + "          }\n"
        + "        },\n"
        + "        \"default_profile\": false,\n"
        + "        \"contributors_enabled\": false,\n"
        + "        \"favourites_count\": 647,\n"
        + "        \"url\": \"http://www.omnitarian.me\",\n"
        + "        \"profile_image_url_https\": \"https://si0.twimg.com/profile_images/1629790393/shrinker_2000_trans_normal.png\",\n"
        + "        \"utc_offset\": -21600,\n"
        + "        \"id\": 37539828,\n"
        + "        \"profile_use_background_image\": true,\n"
        + "        \"listed_count\": 52,\n"
        + "        \"profile_text_color\": \"000000\",\n"
        + "        \"lang\": \"en\",\n"
        + "        \"followers_count\": 608,\n"
        + "        \"protected\": false,\n"
        + "        \"notifications\": null,\n"
        + "        \"profile_background_image_url_https\": \"https://si0.twimg.com/profile_background_images/106455659/rect6056-9.png\",\n"
        + "        \"profile_background_color\": \"EEE3C4\",\n"
        + "        \"verified\": false,\n"
        + "        \"geo_enabled\": false,\n"
        + "        \"time_zone\": \"Central Time (US & Canada)\",\n"
        + "        \"description\": \"Cartoonist, Illustrator, and T-Shirt connoisseur\",\n"
        + "        \"default_profile_image\": false,\n"
        + "        \"profile_background_image_url\": \"http://a0.twimg.com/profile_background_images/106455659/rect6056-9.png\",\n"
        + "        \"statuses_count\": 3575,\n"
        + "        \"friends_count\": 249,\n"
        + "        \"following\": null,\n"
        + "        \"show_all_inline_media\": true,\n"
        + "        \"screen_name\": \"Omnitarian\"\n"
        + "      },\n"
        + "      \"in_reply_to_screen_name\": null,\n"
        + "      \"source\": \"<a>Twitter for iPhone</a>\",\n"
        + "      \"in_reply_to_status_id\": null\n"
        + "    }\n"
        + "  ],\n"
        + "  \"search_metadata\": {\n"
        + "    \"max_id\": 250126199840518145,\n"
        + "    \"since_id\": 24012619984051000,\n"
        + "    \"refresh_url\": \"?since_id=250126199840518145&q=%23freebandnames&result_type=mixed&include_entities=1\",\n"
        + "    \"next_results\": \"?max_id=249279667666817023&q=%23freebandnames&count=4&include_entities=1&result_type=mixed\",\n"
        + "    \"count\": 4,\n"
        + "    \"completed_in\": 0.035,\n"
        + "    \"since_id_str\": \"24012619984051000\",\n"
        + "    \"query\": \"%23freebandnames\",\n"
        + "    \"max_id_str\": \"250126199840518145\"\n"
        + "  }\n"
        + "}";

    String json2 = "[\n"
        + "  {\n"
        + "    \"coordinates\": null,\n"
        + "    \"favorited\": false,\n"
        + "    \"truncated\": false,\n"
        + "    \"created_at\": \"Wed Aug 29 17:12:58 +0000 2012\",\n"
        + "    \"id_str\": \"240859602684612608\",\n"
        + "    \"entities\": {\n"
        + "      \"urls\": [\n"
        + "        {\n"
        + "          \"expanded_url\": \"https://dev.twitter.com/blog/twitter-certified-products\",\n"
        + "          \"url\": \"https://t.co/MjJ8xAnT\",\n"
        + "          \"indices\": [\n"
        + "            52,\n"
        + "            73\n"
        + "          ],\n"
        + "          \"display_url\": \"dev.twitter.com/blog/twitter-c\\u2026\"\n"
        + "        }\n"
        + "      ],\n"
        + "      \"hashtags\": [\n"
        + " \n"
        + "      ],\n"
        + "      \"user_mentions\": [\n"
        + " \n"
        + "      ]\n"
        + "    },\n"
        + "    \"in_reply_to_user_id_str\": null,\n"
        + "    \"contributors\": null,\n"
        + "    \"text\": \"Introducing the Twitter Certified Products Program: https://t.co/MjJ8xAnT\",\n"
        + "    \"retweet_count\": 121,\n"
        + "    \"in_reply_to_status_id_str\": null,\n"
        + "    \"id\": 240859602684612608,\n"
        + "    \"geo\": null,\n"
        + "    \"retweeted\": false,\n"
        + "    \"possibly_sensitive\": false,\n"
        + "    \"in_reply_to_user_id\": null,\n"
        + "    \"place\": null,\n"
        + "    \"user\": {\n"
        + "      \"profile_sidebar_fill_color\": \"DDEEF6\",\n"
        + "      \"profile_sidebar_border_color\": \"C0DEED\",\n"
        + "      \"profile_background_tile\": false,\n"
        + "      \"name\": \"Twitter API\",\n"
        + "      \"profile_image_url\": \"http://a0.twimg.com/profile_images/2284174872/7df3h38zabcvjylnyfe3_normal.png\",\n"
        + "      \"created_at\": \"Wed May 23 06:01:13 +0000 2007\",\n"
        + "      \"location\": \"San Francisco, CA\",\n"
        + "      \"follow_request_sent\": false,\n"
        + "      \"profile_link_color\": \"0084B4\",\n"
        + "      \"is_translator\": false,\n"
        + "      \"id_str\": \"6253282\",\n"
        + "      \"entities\": {\n"
        + "        \"url\": {\n"
        + "          \"urls\": [\n"
        + "            {\n"
        + "              \"expanded_url\": null,\n"
        + "              \"url\": \"http://dev.twitter.com\",\n"
        + "              \"indices\": [\n"
        + "                0,\n"
        + "                22\n"
        + "              ]\n"
        + "            }\n"
        + "          ]\n"
        + "        },\n"
        + "        \"description\": {\n"
        + "          \"urls\": [\n"
        + " \n"
        + "          ]\n"
        + "        }\n"
        + "      },\n"
        + "      \"default_profile\": true,\n"
        + "      \"contributors_enabled\": true,\n"
        + "      \"favourites_count\": 24,\n"
        + "      \"url\": \"http://dev.twitter.com\",\n"
        + "      \"profile_image_url_https\": \"https://si0.twimg.com/profile_images/2284174872/7df3h38zabcvjylnyfe3_normal.png\",\n"
        + "      \"utc_offset\": -28800,\n"
        + "      \"id\": 6253282,\n"
        + "      \"profile_use_background_image\": true,\n"
        + "      \"listed_count\": 10775,\n"
        + "      \"profile_text_color\": \"333333\",\n"
        + "      \"lang\": \"en\",\n"
        + "      \"followers_count\": 1212864,\n"
        + "      \"protected\": false,\n"
        + "      \"notifications\": null,\n"
        + "      \"profile_background_image_url_https\": \"https://si0.twimg.com/images/themes/theme1/bg.png\",\n"
        + "      \"profile_background_color\": \"C0DEED\",\n"
        + "      \"verified\": true,\n"
        + "      \"geo_enabled\": true,\n"
        + "      \"time_zone\": \"Pacific Time (US & Canada)\",\n"
        + "      \"description\": \"The Real Twitter API. I tweet about API changes, service issues and happily answer questions about Twitter and our API. Don't get an answer? It's on my website.\",\n"
        + "      \"default_profile_image\": false,\n"
        + "      \"profile_background_image_url\": \"http://a0.twimg.com/images/themes/theme1/bg.png\",\n"
        + "      \"statuses_count\": 3333,\n"
        + "      \"friends_count\": 31,\n"
        + "      \"following\": null,\n"
        + "      \"show_all_inline_media\": false,\n"
        + "      \"screen_name\": \"twitterapi\"\n"
        + "    },\n"
        + "    \"in_reply_to_screen_name\": null,\n"
        + "    \"in_reply_to_status_id\": null\n"
        + "  },\n"
        + "  {\n"
        + "    \"coordinates\": null,\n"
        + "    \"favorited\": false,\n"
        + "    \"truncated\": false,\n"
        + "    \"created_at\": \"Sat Aug 25 17:26:51 +0000 2012\",\n"
        + "    \"id_str\": \"239413543487819778\",\n"
        + "    \"entities\": {\n"
        + "      \"urls\": [\n"
        + "        {\n"
        + "          \"expanded_url\": \"https://dev.twitter.com/issues/485\",\n"
        + "          \"url\": \"https://t.co/p5bOzH0k\",\n"
        + "          \"indices\": [\n"
        + "            97,\n"
        + "            118\n"
        + "          ],\n"
        + "          \"display_url\": \"dev.twitter.com/issues/485\"\n"
        + "        }\n"
        + "      ],\n"
        + "      \"hashtags\": [\n"
        + " \n"
        + "      ],\n"
        + "      \"user_mentions\": [\n"
        + " \n"
        + "      ]\n"
        + "    },\n"
        + "    \"in_reply_to_user_id_str\": null,\n"
        + "    \"contributors\": null,\n"
        + "    \"text\": \"We are working to resolve issues with application management & logging in to the dev portal: https://t.co/p5bOzH0k ^TS\",\n"
        + "    \"retweet_count\": 105,\n"
        + "    \"in_reply_to_status_id_str\": null,\n"
        + "    \"id\": 239413543487819778,\n"
        + "    \"geo\": null,\n"
        + "    \"retweeted\": false,\n"
        + "    \"possibly_sensitive\": false,\n"
        + "    \"in_reply_to_user_id\": null,\n"
        + "    \"place\": null,\n"
        + "    \"user\": {\n"
        + "      \"profile_sidebar_fill_color\": \"DDEEF6\",\n"
        + "      \"profile_sidebar_border_color\": \"C0DEED\",\n"
        + "      \"profile_background_tile\": false,\n"
        + "      \"name\": \"Twitter API\",\n"
        + "      \"profile_image_url\": \"http://a0.twimg.com/profile_images/2284174872/7df3h38zabcvjylnyfe3_normal.png\",\n"
        + "      \"created_at\": \"Wed May 23 06:01:13 +0000 2007\",\n"
        + "      \"location\": \"San Francisco, CA\",\n"
        + "      \"follow_request_sent\": false,\n"
        + "      \"profile_link_color\": \"0084B4\",\n"
        + "      \"is_translator\": false,\n"
        + "      \"id_str\": \"6253282\",\n"
        + "      \"entities\": {\n"
        + "        \"url\": {\n"
        + "          \"urls\": [\n"
        + "            {\n"
        + "              \"expanded_url\": null,\n"
        + "              \"url\": \"http://dev.twitter.com\",\n"
        + "              \"indices\": [\n"
        + "                0,\n"
        + "                22\n"
        + "              ]\n"
        + "            }\n"
        + "          ]\n"
        + "        },\n"
        + "        \"description\": {\n"
        + "          \"urls\": [\n"
        + " \n"
        + "          ]\n"
        + "        }\n"
        + "      },\n"
        + "      \"default_profile\": true,\n"
        + "      \"contributors_enabled\": true,\n"
        + "      \"favourites_count\": 24,\n"
        + "      \"url\": \"http://dev.twitter.com\",\n"
        + "      \"profile_image_url_https\": \"https://si0.twimg.com/profile_images/2284174872/7df3h38zabcvjylnyfe3_normal.png\",\n"
        + "      \"utc_offset\": -28800,\n"
        + "      \"id\": 6253282,\n"
        + "      \"profile_use_background_image\": true,\n"
        + "      \"listed_count\": 10775,\n"
        + "      \"profile_text_color\": \"333333\",\n"
        + "      \"lang\": \"en\",\n"
        + "      \"followers_count\": 1212864,\n"
        + "      \"protected\": false,\n"
        + "      \"notifications\": null,\n"
        + "      \"profile_background_image_url_https\": \"https://si0.twimg.com/images/themes/theme1/bg.png\",\n"
        + "      \"profile_background_color\": \"C0DEED\",\n"
        + "      \"verified\": true,\n"
        + "      \"geo_enabled\": true,\n"
        + "      \"time_zone\": \"Pacific Time (US & Canada)\",\n"
        + "      \"description\": \"The Real Twitter API. I tweet about API changes, service issues and happily answer questions about Twitter and our API. Don't get an answer? It's on my website.\",\n"
        + "      \"default_profile_image\": false,\n"
        + "      \"profile_background_image_url\": \"http://a0.twimg.com/images/themes/theme1/bg.png\",\n"
        + "      \"statuses_count\": 3333,\n"
        + "      \"friends_count\": 31,\n"
        + "      \"following\": null,\n"
        + "      \"show_all_inline_media\": false,\n"
        + "      \"screen_name\": \"twitterapi\"\n"
        + "    },\n"
        + "    \"in_reply_to_screen_name\": null,\n"
        + "    \"in_reply_to_status_id\": null\n"
        + "  }\n"
        + "]";

    objectMapper.getFactory().enable(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
    objectMapper.getFactory().enable(JsonParser.Feature.ALLOW_COMMENTS);
    objectMapper.getFactory().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
    objectMapper.getFactory().enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
    objectMapper.getFactory().enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);



    int numToPersist = 100;

    for(int i = 0; i < numToPersist; i++) {
      // Create event from json
      Event event = new BaseEvent();
      event.putAll(JsonTupleStore.fromJson(json, objectMapper));

      // Persist event
      store.save(singleton(event));
    }

    for(int i = 0; i < numToPersist; i++) {

      ArrayNode node = (ArrayNode) objectMapper.readTree(json2);
      for(JsonNode node1 : node) {

        // Create event from json
        Event event = new BaseEvent();
        event.putAll(JsonTupleStore.fromJson((ObjectNode) node1));

        // Persist event
        store.save(singleton(event));
      }
    }

    store.flush();

    AccumuloTestUtils.dumpTable(connector, "eventStore_shard");



    Node query = new QueryBuilder()
        .and()
          .eq("entities$urls$indices", 52)
        .end()
        .build();

    CloseableIterable<Event> results = store.query(new Date(0), new Date(System.currentTimeMillis() + 5000), query, null, new Auths());

    assertEquals(numToPersist, Iterables.size(results));
  }

  @Test
  public void test() throws Exception {

    // Nested json
    String json = "{ \"name\":\"Corey\", \"nestedObject\":{\"anotherNest\":{\"innerObj\":\"innerVal\"}}, \"nestedArray\":[\"2\",[[\"4\"],[\"1\"],[\"1\"], \"7\"]], \"ids\":[\"5\",\"2\"], \"locations\":[{\"name\":\"Office\", \"addresses\":[{\"number\":1234,\"street\":\"BlahBlah Lane\"}]}]}";

    // Create event from json
    Event event = new BaseEvent();
    event.putAll(JsonTupleStore.fromJson(json, objectMapper));

    // Persist event
    store.save(singleton(event));
    store.flush();

    Node query = new QueryBuilder()
      .and()
        .eq("name", "Corey")
        .eq("nestedObject$anotherNest$innerObj", "innerVal")
        .eq("nestedArray", "2")
      .end()
    .build();

    CloseableIterable<Event> results = store.query(new Date(currentTimeMillis()-5000), new Date(), query, null, new Auths());

    assertEquals(1, Iterables.size(results));

    for(Event even : results) {

      System.out.println(even);

      System.out.println(JsonTupleStore.toJsonString(even.getTuples(), objectMapper));
    }
  }
}
