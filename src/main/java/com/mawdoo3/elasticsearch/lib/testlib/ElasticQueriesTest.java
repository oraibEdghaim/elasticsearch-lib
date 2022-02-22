package com.mawdoo3.elasticsearch.lib.testlib;

import com.mawdoo3.elasticsearch.lib.beans.BoolSearchingCriteria;
import com.mawdoo3.elasticsearch.lib.beans.RangeCriteria;
import com.mawdoo3.elasticsearch.lib.beans.SearchingCriteria;
import com.mawdoo3.elasticsearch.lib.beans.enums.BoolQueryClause;
import com.mawdoo3.elasticsearch.lib.elastic.ElasticQueries;
import com.mawdoo3.elasticsearch.lib.elastic.ElasticServerConnection;
import com.mawdoo3.elasticsearch.lib.exceptions.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.HashMap;
import java.util.Map;

public class ElasticQueriesTest {

    private static final String host = "localhost";
    private static final int port = 9200;
    private static final String schema = "http";

   public static void main(String[] args) throws ElasticSearchException {
        RestHighLevelClient client = ElasticServerConnection.getInstance(host,port,schema).getClient();
        System.out.println("Start testing the elastic search queries ");

       SearchingCriteria searchCriteria = new SearchingCriteria();
       searchCriteria.fieldName("Company Name");
       searchCriteria.fieldValue("Data ");
       searchCriteria.minimumShouldMatch("1");
      SearchResponse response =  ElasticQueries.getMatchedDocuments(client,"salary",searchCriteria);
      System.out.println("The number of hits " + response.getHits().getTotalHits());

       searchCriteria.slop(0);
       SearchResponse response2 =  ElasticQueries.getMatchedPhraseDocuments(client,"salary",searchCriteria);
       System.out.println("The number of hits " + response2.getHits().getTotalHits());

       SearchResponse response3 =  ElasticQueries.getMatchedPhrasePrefixDocuments(client,"salary",searchCriteria);
       System.out.println("The number of hits " + response3.getHits().getTotalHits());

       String [] fields = {"Company Name","Job Title"};
       SearchResponse response4 =  ElasticQueries.getMatchedDocuments(client,"salary","Data",fields,searchCriteria);
       System.out.println("The number of hits " + response4.getHits().getTotalHits());


       searchCriteria.fieldValue("Da*");
       SearchResponse response5=  ElasticQueries.getMatchedWildCardDocuments(client,"salary",searchCriteria);
       System.out.println("The number of hits " + response5.getHits().getTotalHits());


       SearchingCriteria mustSearch = new SearchingCriteria();
       mustSearch.fieldName("Company Name");
       mustSearch.fieldValue("Google");
       mustSearch.boost(2);

       SearchingCriteria should = new SearchingCriteria();
       should.fieldName("Location");
       should.fieldValue("Bangalore");
       should.boost(3);

       SearchingCriteria mustNot = new SearchingCriteria();
       mustNot.fieldName("Job Title");
       mustNot.fieldValue("Data Engineer");
       mustNot.boost(1);

       BoolSearchingCriteria bool = new BoolSearchingCriteria();
       bool.addBoolCriteria(BoolQueryClause.MUST,mustSearch);
       bool.addBoolCriteria(BoolQueryClause.SHOULD,should);
       bool.addBoolCriteria(BoolQueryClause.MUST_NOT,mustNot);


       SearchResponse response6=  ElasticQueries.getMatchedBooleanClausesDocuments(client,"salary",bool);
       System.out.println("The number of hits " + response6.getHits().getTotalHits());

       SearchingCriteria rangeCriteria = new SearchingCriteria();
       rangeCriteria.fieldName("Salaries Reported");

       RangeCriteria range = new RangeCriteria();
       range.setLt(20);
       range.setGte(10);
       rangeCriteria.setRangeCriteria(range);

       SearchResponse response7=  ElasticQueries.getMatchedRangeDocuments(client,"salary",rangeCriteria);
       System.out.println("The number of hits " + response7.getHits().getTotalHits());


   }
}
