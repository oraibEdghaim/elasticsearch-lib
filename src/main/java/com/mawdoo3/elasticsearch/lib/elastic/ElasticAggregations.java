package com.mawdoo3.elasticsearch.lib.elastic;

import com.mawdoo3.elasticsearch.lib.beans.AggregationCriteria;
import com.mawdoo3.elasticsearch.lib.exceptions.ElasticSearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
/**
 * TODO LIST :
 * - AVG Aggregation : These values can be generated by a provided script.
 * - Count : ------------------------------
 * Handle the runtime script field for a metrics aggregation
 * Handle Histogram fields for the metrics aggregation
 * Handle sorting of bucket aggregations (4 types)
 * */

/**
 *<p>ElasticAggregations class which supports some functionalities to summarizes your data as metrics, statistics, or other analytics!</p>
 <li> Metric aggregations</li>
 <li> Buckets aggregations</li>
 <li> Combined aggregations</li>
 * <br/>
 * @author  Oraib Edghaim
 * @version 6.8.10
 */
public class ElasticAggregations {

    private static final Logger logger = LoggerFactory.getLogger(ElasticAggregations.class);
    private ElasticAggregations(){}

    /**
     * <p> returns the minimum value among numeric values extracted from the aggregated documents.  </p>
     * <p> A single-value metrics aggregation</p>
     * @return double
     * */
    public static double getMinAggregationValue(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MinAggregationBuilder minBuilder = new MinAggregationBuilder(aggregationCriteria.getAggregationName());
        minBuilder.field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() != null){
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(minBuilder);
        String format =  String.format("Method : getMinAggregationValue , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Min result = response.getAggregations().get(aggregationCriteria.getAggregationName());
        return result.getValue();
    }
    /**
     * <p> returns the maximum value among numeric values extracted from the aggregated documents.  </p>
     * <p> A single-value metrics aggregation</p>
     * @return double
     * */
    public static double getMaxAggregationValue(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        MaxAggregationBuilder maxBuilder = new MaxAggregationBuilder(aggregationCriteria.getAggregationName());
        maxBuilder.field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() != null){
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(maxBuilder);

        String format =  String.format("Method : getMaxAggregationValue , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Max result = response.getAggregations().get(aggregationCriteria.getAggregationName());
        return result.getValue();
    }
    /**
     * <p> returns the average that computes the average of numeric values that are extracted from the aggregated documents </p>
     * <p> A single-value metrics aggregation</p>
     * @return double
     * */
    public static double getAvgAggregationValue(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AvgAggregationBuilder avgBuilder = new AvgAggregationBuilder(aggregationCriteria.getAggregationName());
        avgBuilder.field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() != null){
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(avgBuilder);

        String format =  String.format("Method : getAvgAggregationValue , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Avg result = response.getAggregations().get(aggregationCriteria.getAggregationName());
        return result.getValue();
    }
    /**
     * <p> that computes stats over numeric values extracted from the aggregated documents.
     * <p> A multi-value metrics aggregation</p>
     * <p> Note : The stats that are returned consist of: min, max, sum, count and avg.</p>
     * @return Stats
     * */
    public static Stats getStatsAggregationValue(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        StatsAggregationBuilder statsBuilder = new StatsAggregationBuilder(aggregationCriteria.getAggregationName());
        statsBuilder.field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() != null){
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(statsBuilder);

        String format =  String.format("Method : getStatsAggregationValue , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        return response.getAggregations().get(aggregationCriteria.getAggregationName());
    }
    /**
     * <p> Returns a value that counts the number of values that are extracted from the aggregated documents
     * <p> A single-value metrics aggregation</p>
     * @return long
     * */
    public static long getCountAggregationValue(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        ValueCountAggregationBuilder countBuilder = AggregationBuilders.count(aggregationCriteria.getAggregationName());
        countBuilder.field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() != null){
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(countBuilder);
        String format =  String.format("Method : getCountAggregationValue , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        ValueCount result = response.getAggregations().get(aggregationCriteria.getAggregationName());
        return result.getValue();
    }
    /**
     * <p> Returns a value that sums up numeric values that are extracted from the aggregated documents
     * <p> A single-value metrics aggregation</p>
     * @return double
     * */
    public static double getSumAggregationValue(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SumAggregationBuilder sumBuilder = AggregationBuilders.sum(aggregationCriteria.getAggregationName());
        sumBuilder.field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() != null){
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(sumBuilder);
        String format =  String.format("Method : getSumAggregationValue , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Sum result = response.getAggregations().get(aggregationCriteria.getAggregationName());
        return result.getValue();
    }
    /**
     * <p> Returns a value that computes the count of unique values for a given field.
     * @return long
     * */
    public static long getCardinalityAggregation(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        CardinalityAggregationBuilder cardinalityBuilder = AggregationBuilders.cardinality(aggregationCriteria.getAggregationName());
        cardinalityBuilder.field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() != null){
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(cardinalityBuilder);
        String format =  String.format("Method : getCardinalityAggregation , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Cardinality result = response.getAggregations().get(aggregationCriteria.getAggregationName());
        return result.getValue();
    }
    /**
     * <p>Returns map of the buckets( key and documents count) for each bucket by grouping the data based on time interval.
     * @return Map<String,Long>
     * */
    public static Map<String,Long> getDateHistogramBuckets(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        DateHistogramAggregationBuilder histogramAggregationBuilder = new DateHistogramAggregationBuilder(aggregationCriteria.getAggregationName());
        histogramAggregationBuilder.field(aggregationCriteria.getFieldName());
        histogramAggregationBuilder.dateHistogramInterval(aggregationCriteria.getTimeInterval());
        if(aggregationCriteria.getSizeOfHits() !=null) {
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(histogramAggregationBuilder);
        String format =  String.format("Method : getDateHistogramBuckets , Query details : %s ",searchSourceBuilder);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Aggregations aggregations = response.getAggregations();
        Histogram dateHistogram = aggregations.get(aggregationCriteria.getFieldName());
        Map<String,Long> dateHistogramBuckets = new HashMap<>();
        if(dateHistogram != null) {
            dateHistogram.getBuckets().forEach(bucket -> dateHistogramBuckets.put(bucket.getKeyAsString(), bucket.getDocCount()));
        }
        return dateHistogramBuckets;
    }
    /**
     * <p>Returns map of the buckets( key and documents count) for each bucket by grouping the data based on any numerical interval.
     * @return Map<String,Long>
     * */
    public static Map<String, Long> getHistogramBuckets(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        HistogramAggregationBuilder histogramBuilder = new HistogramAggregationBuilder(aggregationCriteria.getAggregationName());
        histogramBuilder.field(aggregationCriteria.getFieldName()).interval(aggregationCriteria.getFixedInterval());
        if(aggregationCriteria.getSizeOfHits() !=null) {
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(histogramBuilder);
        String format =  String.format("Method : getHistogramBuckets , Query details : %s ",searchSourceBuilder);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Aggregations aggregations = response.getAggregations();
        Histogram histogramResults = aggregations.get(aggregationCriteria.getAggregationName());

        Map<String,Long> buckets = new HashMap<>();
        if(histogramResults !=null) {
            histogramResults.getBuckets().forEach(bucket -> buckets.put(bucket.getKeyAsString(), bucket.getDocCount()));
        }
        return buckets;
    }

    /**
     * <p>Returns map of the buckets( key and documents count) for each bucket by grouping the data based on range intervals.
     * @return Map<String,Long>
     * */
    public static Map<String, Long> getRangeBuckets(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        RangeAggregationBuilder rangeBuilder = new RangeAggregationBuilder(aggregationCriteria.getAggregationName());
        rangeBuilder.field(aggregationCriteria.getFieldName());
        aggregationCriteria.getRanges().forEach(range -> rangeBuilder.addRange(range));
        if(aggregationCriteria.getSizeOfHits() !=null) {
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(rangeBuilder);
        String format =  String.format("Method : getRangeBuckets , Query details : %s ",searchSourceBuilder);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Aggregations aggregations = response.getAggregations();
        Range rangeBuckets = aggregations.get(aggregationCriteria.getAggregationName());
        Map<String,Long> buckets =  new HashMap<>();
        if(rangeBuckets!=null) {
            rangeBuckets.getBuckets().forEach(bucket -> buckets.put(bucket.getKeyAsString(), bucket.getDocCount()));
        }
        return buckets;
    }

    /**
     * <p>Returns map of the buckets( key and documents count) for every unique term it encounters for the specified field.
     * @return Map<String,Long>
     * */
    public static Map<String, Long> getTermBuckets(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termBuilder = AggregationBuilders.terms(aggregationCriteria.getAggregationName()).field(aggregationCriteria.getFieldName());
        if(aggregationCriteria.getSizeOfHits() !=null) {
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(termBuilder);
        String format =  String.format("Method : getTermsAggBuckets , Query details : %s ",searchSourceBuilder);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : "+indexName,e);
        }
        Aggregations aggregations = response.getAggregations();
        Terms result = aggregations.get(aggregationCriteria.getAggregationName());
        Map<String,Long> buckets =  new HashMap<>();
        result.getBuckets().forEach(bucket -> buckets.put(bucket.getKeyAsString(),bucket.getDocCount()));
        return buckets;
    }
    /**
     * <p>Returns the count of the filtered documents to only those that match one of the given terms and is not analyzed.
     * @return long
     * */
    public static long getFilteredTermsCount(RestHighLevelClient client, String indexName, AggregationCriteria aggregationCriteria) throws ElasticSearchException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder termBuilder = QueryBuilders.termQuery(aggregationCriteria.getFieldName(),aggregationCriteria.getFieldValue());
        FilterAggregationBuilder filter = new FilterAggregationBuilder(aggregationCriteria.getAggregationName(), termBuilder);
        if(aggregationCriteria.getSizeOfHits() !=null) {
            searchSourceBuilder.size(aggregationCriteria.getSizeOfHits());
        }
        searchSourceBuilder.aggregation(filter);
        String format =  String.format("Method : getFilteredTermsCount , Query details : %s ",searchSourceBuilder);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the aggregation result from index : " + indexName,e);
        }
        Aggregations aggregations = response.getAggregations();
        ParsedFilter result = aggregations.get(aggregationCriteria.getAggregationName());
        return result.getDocCount();
    }
}
