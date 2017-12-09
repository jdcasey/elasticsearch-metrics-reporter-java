/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
import org.commonjava.util.jhttpc.HttpFactory;
import org.commonjava.util.jhttpc.JHttpCException;
import org.commonjava.util.jhttpc.model.SiteConfig;
import org.commonjava.util.jhttpc.util.UrlUtils;
import org.elasticsearch.metrics.percolation.Notifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.metrics.JsonMetrics.JsonCounter;
import static org.elasticsearch.metrics.JsonMetrics.JsonGauge;
import static org.elasticsearch.metrics.JsonMetrics.JsonHistogram;
import static org.elasticsearch.metrics.JsonMetrics.JsonMeter;
import static org.elasticsearch.metrics.JsonMetrics.JsonMetric;
import static org.elasticsearch.metrics.JsonMetrics.JsonTimer;
import static org.elasticsearch.metrics.MetricsElasticsearchModule.BulkIndexOperationHeader;

public class ElasticsearchReporter
        extends ScheduledReporter
{

    private static final String HEADER_CONTENT_TYPE = "Content-Type";

    private static final String APPLICATION_JSON = "application/json";

    private static final TypeReference<Map<String, Object>> PERCOLATE_RESPONSE_TYPE = new TypeReference<Map<String, Object>>(){};

    public static Builder forRegistry( MetricRegistry registry )
    {
        return new Builder( registry );
    }

    public static class Builder
    {
        private final MetricRegistry registry;

        private Clock clock;

        private String prefix;

        private TimeUnit rateUnit;

        private TimeUnit durationUnit;

        private MetricFilter filter;

        private String[] hosts = new String[] { "localhost:9200" };

        private String index = "metrics";

        private String indexDateFormat = "yyyy-MM";

        private int bulkSize = 2500;

        private Notifier percolationNotifier;

        private MetricFilter percolationFilter;

        private int timeout = 1000;

        private String timestampFieldname = "@timestamp";

        private Map<String, Object> additionalFields;

        private HttpFactory httpFactory;

        private SiteConfig siteConfig;

        private Builder( MetricRegistry registry )
        {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public Builder withHttpFactory( HttpFactory httpFactory )
        {
            this.httpFactory = httpFactory;
            return this;
        }

        public Builder withSiteConfig( SiteConfig siteConfig )
        {
            this.siteConfig = siteConfig;
            return this;
        }

        /**
         * Inject your custom definition of how time passes. Usually the default clock is sufficient
         */
        public Builder withClock( Clock clock )
        {
            this.clock = clock;
            return this;
        }

        /**
         * Configure a prefix for each metric name. Optional, but useful to identify single hosts
         */
        public Builder prefixedWith( String prefix )
        {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert all the rates to a certain timeunit, defaults to seconds
         */
        public Builder convertRatesTo( TimeUnit rateUnit )
        {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert all the durations to a certain timeunit, defaults to milliseconds
         */
        public Builder convertDurationsTo( TimeUnit durationUnit )
        {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Allows to configure a special MetricFilter, which defines what metrics are reported
         */
        public Builder filter( MetricFilter filter )
        {
            this.filter = filter;
            return this;
        }

        /**
         * Configure an array of hosts to send data to.
         * Note: Data is always sent to only one host, but this makes sure, that even if a part of your elasticsearch cluster
         * is not running, reporting still happens
         * A host must be in the format hostname:port
         * The port must be the HTTP port of your elasticsearch instance
         */
        public Builder hosts( String... hosts )
        {
            this.hosts = hosts;
            return this;
        }

        /**
         * The timeout to wait for until a connection attempt is and the next host is tried
         */
        public Builder timeout( int timeout )
        {
            this.timeout = timeout;
            return this;
        }

        /**
         * The index name to index in
         */
        public Builder index( String index )
        {
            this.index = index;
            return this;
        }

        /**
         * The index date format used for rolling indices
         * This is appended to the index name, split by a '-'
         */
        public Builder indexDateFormat( String indexDateFormat )
        {
            this.indexDateFormat = indexDateFormat;
            return this;
        }

        /**
         * The bulk size per request, defaults to 2500 (as metrics are quite small)
         */
        public Builder bulkSize( int bulkSize )
        {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * A metrics filter to define the metrics which should be used for percolation/notification
         */
        public Builder percolationFilter( MetricFilter percolationFilter )
        {
            this.percolationFilter = percolationFilter;
            return this;
        }

        /**
         * An instance of the notifier implemention which should be executed in case of a matching percolation
         */
        public Builder percolationNotifier( Notifier notifier )
        {
            this.percolationNotifier = notifier;
            return this;
        }

        /**
         * Configure the name of the timestamp field, defaults to '@timestamp'
         */
        public Builder timestampFieldname( String fieldName )
        {
            this.timestampFieldname = fieldName;
            return this;
        }

        /**
         * Additional fields to be included for each metric
         *
         * @param additionalFields
         *
         * @return
         */
        public Builder additionalFields( Map<String, Object> additionalFields )
        {
            this.additionalFields = additionalFields;
            return this;
        }

        public ElasticsearchReporter build()
                throws IOException
        {
            return new ElasticsearchReporter( registry, hosts, timeout, index, indexDateFormat, bulkSize, clock, prefix,
                                              rateUnit, durationUnit, filter, percolationFilter, percolationNotifier,
                                              timestampFieldname, additionalFields, httpFactory, siteConfig );
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( ElasticsearchReporter.class );

    private final String[] hosts;

    private final Clock clock;

    private final String prefix;

    private final String index;

    private final int bulkSize;

    private final int timeout;

    private HttpFactory httpFactory;

    private SiteConfig siteConfig;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ObjectWriter writer;

    private MetricFilter percolationFilter;

    private Notifier notifier;

    private String currentIndexName;

    private SimpleDateFormat indexDateFormat = null;

    private boolean checkedForIndexTemplate = false;

    public ElasticsearchReporter( MetricRegistry registry, String[] hosts, int timeout, String index,
                                  String indexDateFormat, int bulkSize, Clock clock, String prefix, TimeUnit rateUnit,
                                  TimeUnit durationUnit, MetricFilter filter, MetricFilter percolationFilter,
                                  Notifier percolationNotifier, String timestampFieldname,
                                  Map<String, Object> additionalFields, final HttpFactory httpFactory,
                                  final SiteConfig siteConfig )
            throws MalformedURLException
    {
        super( registry, "elasticsearch-reporter", filter, rateUnit, durationUnit );
        this.hosts = hosts;
        this.index = index;
        this.bulkSize = bulkSize;
        this.clock = clock;
        this.prefix = prefix;
        this.timeout = timeout;
        this.httpFactory = httpFactory;
        this.siteConfig = siteConfig;

        if ( indexDateFormat != null && indexDateFormat.length() > 0 )
        {
            this.indexDateFormat = new SimpleDateFormat( indexDateFormat );
        }
        if ( percolationNotifier != null && percolationFilter != null )
        {
            this.percolationFilter = percolationFilter;
            this.notifier = percolationNotifier;
        }
        if ( timestampFieldname == null || timestampFieldname.trim().length() == 0 )
        {
            LOGGER.error( "Timestampfieldname {} is not valid, using default @timestamp", timestampFieldname );
            timestampFieldname = "@timestamp";
        }

        objectMapper.configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false );
        objectMapper.configure( SerializationFeature.CLOSE_CLOSEABLE, false );
        // auto closing means, that the objectmapper is closing after the first write call, which does not work for bulk requests
        objectMapper.configure( JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false );
        objectMapper.configure( JsonGenerator.Feature.AUTO_CLOSE_TARGET, false );
        objectMapper.registerModule( new AfterburnerModule() );
        objectMapper.registerModule(
                new MetricsElasticsearchModule( rateUnit, durationUnit, timestampFieldname, additionalFields ) );
        writer = objectMapper.writer();
        checkForIndexTemplate();
    }

    @Override
    public void report( SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                        SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
                        SortedMap<String, Timer> timers )
    {

        // nothing to do if we dont have any metrics to report
        if ( gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() && timers.isEmpty() )
        {
            LOGGER.info( "All metrics empty, nothing to report" );
            return;
        }

        if ( !checkedForIndexTemplate )
        {
            checkForIndexTemplate();
        }
        final long timestamp = clock.getTime() / 1000;

        currentIndexName = index;
        if ( indexDateFormat != null )
        {
            currentIndexName += "-" + indexDateFormat.format( new Date( timestamp * 1000 ) );
        }

        List<JsonMetric> percolationMetrics = new ArrayList<>();
        AtomicInteger entryCounter = new AtomicInteger( 0 );

        Set<Set<JsonMetric<?>>> metricSets = new HashSet<>();
        Set<JsonMetric<?>> current = new HashSet<>();
        for ( Map.Entry<String, Gauge> entry : gauges.entrySet() )
        {
            if ( entry.getValue().getValue() != null )
            {
                JsonMetric jsonMetric = new JsonGauge( name( prefix, entry.getKey() ), timestamp, entry.getValue() );
                current = addMetricToSet( jsonMetric, current, metricSets, entryCounter );
                addJsonMetricToPercolationIfMatching( jsonMetric, percolationMetrics );
            }
        }

        for ( Map.Entry<String, Counter> entry : counters.entrySet() )
        {
            JsonCounter jsonMetric = new JsonCounter( name( prefix, entry.getKey() ), timestamp, entry.getValue() );
            current = addMetricToSet( jsonMetric, current, metricSets, entryCounter );
            addJsonMetricToPercolationIfMatching( jsonMetric, percolationMetrics );
        }

        for ( Map.Entry<String, Histogram> entry : histograms.entrySet() )
        {
            JsonHistogram jsonMetric = new JsonHistogram( name( prefix, entry.getKey() ), timestamp, entry.getValue() );
            current = addMetricToSet( jsonMetric, current, metricSets, entryCounter );
            addJsonMetricToPercolationIfMatching( jsonMetric, percolationMetrics );
        }

        for ( Map.Entry<String, Meter> entry : meters.entrySet() )
        {
            JsonMeter jsonMetric = new JsonMeter( name( prefix, entry.getKey() ), timestamp, entry.getValue() );
            current = addMetricToSet( jsonMetric, current, metricSets, entryCounter );
            addJsonMetricToPercolationIfMatching( jsonMetric, percolationMetrics );
        }

        for ( Map.Entry<String, Timer> entry : timers.entrySet() )
        {
            JsonTimer jsonMetric = new JsonTimer( name( prefix, entry.getKey() ), timestamp, entry.getValue() );
            current = addMetricToSet( jsonMetric, current, metricSets, entryCounter );
            addJsonMetricToPercolationIfMatching( jsonMetric, percolationMetrics );
        }

        /* @formatter:off */
        withClient( ( client, context ) ->
        {
            metricSets.forEach( metricSet ->
            {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                AtomicBoolean proceed = new AtomicBoolean( true );
                metricSet.forEach( metric ->
                {
                    try
                    {
                       writer.writeValue( out,
                                          new BulkIndexOperationHeader(
                                                  currentIndexName,
                                                  metric.type() ) );
                       out.write( "\n".getBytes() );
                       writer.writeValue( out, metric );
                       out.write( "\n".getBytes() );

                       out.flush();
                    }
                    catch ( IOException e )
                    {
                       LOGGER.error(
                               "Failed to serialize metrics to stream.",
                               e );
                       proceed.set( false );
                    }
                } );

                forHosts( ( host ) ->
                {
                    HttpPost post = new HttpPost( UrlUtils.buildUrl( host, "/_bulk" ) );

                    post.setEntity( new InputStreamEntity(new ByteArrayInputStream( out.toByteArray() ), out.size() ) );
                    post.setHeader( HEADER_CONTENT_TYPE, APPLICATION_JSON );

                    try(CloseableHttpResponse response = client.execute( post, context ))
                    {
                        if ( response.getStatusLine().getStatusCode() == SC_OK )
                        {
                            return true;
                        }

                        LOGGER.trace( "Could not post metrics to: {}. Response was: {}", host, response.getStatusLine() );
                    }

                    return false;
                } );
            } );

            // execute the notifier impl, in case percolation found matches
            if ( percolationMetrics.size() > 0 && notifier != null )
            {
                for ( JsonMetric jsonMetric : percolationMetrics )
                {
                    List<String> matches = getPercolationMatches( jsonMetric );
                    for ( String match : matches )
                    {
                        notifier.notify( jsonMetric, match );
                    }
                }
            }
        } );
        /* @formatter:on */
    }

    /**
     * Add metric to list of matched percolation if needed
     */
    private void addJsonMetricToPercolationIfMatching( JsonMetric<? extends Metric> jsonMetric,
                                                       List<JsonMetric> percolationMetrics )
    {
        if ( percolationFilter != null && percolationFilter.matches( jsonMetric.name(), jsonMetric.value() ) )
        {
            percolationMetrics.add( jsonMetric );
        }
    }

    private void forHosts( HostFunction func )
    {
        boolean posted = false;
        for ( String host : hosts )
        {
            try
            {
                func.execute( host );
            }
            catch ( MalformedURLException e )
            {
                LOGGER.debug( "Invalid Elasticsearch URL: {}/_bulk", host );
            }
            catch ( ClientProtocolException e )
            {
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( String.format( "Could not talk to: %s.", host ), e );
                }
            }
            catch ( IOException e )
            {
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( String.format( "Could not talk to: %s.", host ), e );
                }
            }
        }

        if ( !posted )
        {
            LOGGER.debug( "Cannot find viable Elasticsearch server to talk to in: {}", hosts );
        }
    }

    private void withClient( ClientFunction func )
    {
        try (CloseableHttpClient client = httpFactory.createClient( siteConfig ))
        {
            HttpContext context = httpFactory.createContext( siteConfig );

            func.withClient( client, context );

            // catch the exception to make sure we do not interrupt the live application
        }
        catch ( IOException e )
        {
            LOGGER.error( "Couldnt report to elasticsearch server", e );
        }
        catch ( JHttpCException e )
        {
            LOGGER.error( "Couldnt report to elasticsearch server", e );
        }
    }

    private interface HostFunction
    {
        boolean execute( String host )
                throws MalformedURLException, ClientProtocolException, IOException;
    }

    private interface ClientFunction
    {
        void withClient( CloseableHttpClient client, HttpContext context )
                throws JHttpCException, IOException;
    }

    private Set<JsonMetric<?>> addMetricToSet( final JsonMetric metric, Set<JsonMetric<?>> current,
                                               final Set<Set<JsonMetric<?>>> metricSets,
                                               final AtomicInteger entryCounter )
    {
        if ( entryCounter.incrementAndGet() % bulkSize == 0 )
        {
            metricSets.add( current );
            current = new HashSet<>();
        }

        current.add( metric );

        return current;
    }

    /**
     * Execute a percolation request for the specified metric
     */
    private List<String> getPercolationMatches( JsonMetric jsonMetric )
            throws IOException
    {
        final List<String> matches = new ArrayList<>();
        /* @formatter:off */
        withClient( ( client, context ) ->
        {
            Map<String, Object> data = new HashMap<>( 1 );
            data.put( "doc", jsonMetric );

            String json = objectMapper.writeValueAsString( data );

            forHosts( ( host ) ->
            {
                String url = UrlUtils.buildUrl( host, currentIndexName, jsonMetric.type(),
                                              "_percolate" );

                HttpPost post = new HttpPost( url );
                post.setEntity( new StringEntity( json ) );
                post.setHeader( HEADER_CONTENT_TYPE, APPLICATION_JSON );

                try (CloseableHttpResponse response = client.execute( post, context ))
                {
                    if ( response.getStatusLine().getStatusCode() != 200 )
                    {
                        LOGGER.trace( "Error percolating to: {}. Status: {}", host, response.getStatusLine() );
                        return false;
                    }

                    Map<String, Object> input =
                            objectMapper.readValue( response.getEntity().getContent(), PERCOLATE_RESPONSE_TYPE );

                    if ( input.containsKey( "matches" ) && input.get(
                          "matches" ) instanceof List )
                    {
                        ( (List<Map<String,String>>) input.get("matches") ).stream()
                                                                           .filter( entry->entry.containsKey( "_id" ) )
                                                                           .forEach( entry->matches.add( entry.get("_id") ) );
                    }

                    return true;
                }
            } );
        } );
        /* @formatter:on */

        return matches;
    }

    /**
     * This index template is automatically applied to all indices which start with the index name
     * The index template simply configures the name not to be analyzed
     */
    private void checkForIndexTemplate()
    {
        /* @formatter:off */
        withClient( (client, context)->
        {
            forHosts( host->
            {
                boolean templateMissing = false;

                String url = UrlUtils.buildUrl( host, "/_template/metrics_template" );

                HttpHead head = new HttpHead( url );
                try(CloseableHttpResponse response = client.execute( head, context ) )
                {
                    templateMissing = response.getStatusLine().getStatusCode() == SC_NOT_FOUND;
                }

                if ( templateMissing )
                {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    JsonGenerator json = new JsonFactory().createGenerator( baos );
                    json.writeStartObject();
                    json.writeStringField( "template", index + "*" );
                    json.writeObjectFieldStart( "mappings" );

                    json.writeObjectFieldStart( "_default_" );
                    json.writeObjectFieldStart( "_all" );
                    json.writeBooleanField( "enabled", false );
                    json.writeEndObject();
                    json.writeObjectFieldStart( "properties" );
                    json.writeObjectFieldStart( "name" );
                    json.writeObjectField( "type", "string" );
                    json.writeObjectField( "index", "not_analyzed" );
                    json.writeEndObject();
                    json.writeEndObject();
                    json.writeEndObject();

                    json.writeEndObject();
                    json.writeEndObject();
                    json.flush();

                    HttpPut put = new HttpPut( url );
                    put.setEntity( new InputStreamEntity( new ByteArrayInputStream( baos.toByteArray() ), baos.size() ) );
                    put.setHeader( HEADER_CONTENT_TYPE, APPLICATION_JSON );

                    try(CloseableHttpResponse response = client.execute( put, context ))
                    {
                        // REST principles normally dictates PUT -> 201 Created response
                        if ( response.getStatusLine().getStatusCode() != SC_OK || response.getStatusLine().getStatusCode() != SC_CREATED )
                        {
                            LOGGER.error( "Error adding metrics template to elasticsearch: {}", response.getStatusLine() );
                            return false;
                        }
                    }
                }

                return true;
            } );
        } );
        /* @formatter:on */
    }
}
