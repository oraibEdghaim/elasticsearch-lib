package com.mawdoo3.elasticsearch.lib.testlib;

import com.mawdoo3.elasticsearch.lib.beans.AggregationCriteria;
import com.mawdoo3.elasticsearch.lib.elastic.ElasticAggregations;
import com.mawdoo3.elasticsearch.lib.elastic.ElasticServerConnection;
import com.mawdoo3.elasticsearch.lib.exceptions.ElasticSearchException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticAggregationTest {

    private static final String host = "localhost";
    private static final int port = 9200;
    private static final String schema = "http";
    private static String INDEX ="salary";

    public static void main(String[] args) throws ElasticSearchException {
        RestHighLevelClient client = ElasticServerConnection.getInstance(host,port,schema).getClient();
        System.out.println("Start testing the elastic aggregations ");

        AggregationCriteria aggregationCriteria = new AggregationCriteria();
        aggregationCriteria.setFieldName("Salaries Reported");
        aggregationCriteria.setSizeOfHits(0);

        double min = ElasticAggregations.getMinAggregationValue(client,INDEX,aggregationCriteria);
        System.out.println("The min value for the agg function : " + min);

        double max = ElasticAggregations.getMaxAggregationValue(client,INDEX,aggregationCriteria);
        System.out.println("The max  value for the agg function : " + max);

        double sum = ElasticAggregations.getSumAggregationValue(client,INDEX,aggregationCriteria);
        System.out.println("The sum value for the agg function : " + sum);

        double avg = ElasticAggregations.getAvgAggregationValue(client,INDEX,aggregationCriteria);
        System.out.println("The avg value for the agg function : " + avg);

        double count = ElasticAggregations.getCountAggregationValue(client,INDEX,aggregationCriteria);
        System.out.println("The count value for the agg function : " + count);

        Stats stats = ElasticAggregations.getStatsAggregationValue(client,INDEX,aggregationCriteria);
        System.out.println("The stats value for the agg function : " + stats.getAvg() + ": " + stats.getMaxAsString());

        long cardinality = ElasticAggregations.getCardinalityAggregation(client,INDEX,aggregationCriteria);
        System.out.println("The cardinality value for the agg function : " + cardinality);


        aggregationCriteria.setFIxedInterval(10);
        Map<String,Long> histogramBuckets = ElasticAggregations.getHistogramBuckets(client,INDEX,aggregationCriteria);
        histogramBuckets.keySet().forEach(key -> System.out.println("The value of key " + key + " : " + histogramBuckets.get(key) ));

        /*aggregationCriteria.setFieldName("Salary");
        aggregationCriteria.setTimeInterval(DateHistogramInterval.YEAR);
        Map<String,Long> dateHistogramBuckets = ElasticAggregations.getDateHistogramBuckets(client,INDEX,aggregationCriteria);
        dateHistogramBuckets.keySet().forEach(key -> System.out.println("The value of key " + key + " : " + dateHistogramBuckets.get(key) ));
      */
        List<RangeAggregator.Range> ranges =  new ArrayList<>();
        RangeAggregator.Range range=  new  RangeAggregator.Range("1",10.0,100.0);
        RangeAggregator.Range range2=  new  RangeAggregator.Range("2",100.0,200.0);
        ranges.add(range);
        ranges.add(range2);
        aggregationCriteria.setRanges(ranges);

        Map<String,Long> rangeBuckets= ElasticAggregations.getRangeBuckets(client,INDEX,aggregationCriteria);
        rangeBuckets.keySet().forEach(key -> System.out.println("The value of key " + key + " : " + rangeBuckets.get(key) ));

        aggregationCriteria.setFieldName("Location");
        Map<String,Long> termBuckets= ElasticAggregations.getTermBuckets(client,INDEX,aggregationCriteria);
        termBuckets.keySet().forEach(key -> System.out.println("The value of key " + key + " : " + termBuckets.get(key) ));

        aggregationCriteria.setFieldValue("Mumbai");
        long filteredTerm = ElasticAggregations.getFilteredTermsCount(client,INDEX,aggregationCriteria);
        System.out.println("The filtered term count : "+filteredTerm);

    }

}
