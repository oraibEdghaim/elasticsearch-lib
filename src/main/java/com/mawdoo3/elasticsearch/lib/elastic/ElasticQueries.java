package com.mawdoo3.elasticsearch.lib.elastic;

import com.mawdoo3.elasticsearch.lib.beans.BoolSearchingCriteria;
import com.mawdoo3.elasticsearch.lib.beans.SearchingCriteria;
import com.mawdoo3.elasticsearch.lib.beans.enums.BoolQueryClause;
import com.mawdoo3.elasticsearch.lib.exceptions.ElasticSearchException;
import com.mawdoo3.elasticsearch.lib.utils.util;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * <p>ElasticQueries class which supports the below functionalities:</p>
 * <li>CRUD Operations with Elasticsearch based on defined document id, index name and type name.</li>
 * <li>Full Text Queries with Elasticsearch based on some defined criteria. </li>
 * <br/>
 * @author  Oraib Edghaim
 * @version 6.8.10
 */
public class ElasticQueries {

    private static final Logger logger = LoggerFactory.getLogger(ElasticQueries.class);
    private ElasticQueries(){}

    /**
     * <p> Insert a new document with a defined id into a specific type and index. </p>
     * <p> Note : The document should be passed as a Json string </p>
     * @return IndexResponse
     * */
    public static IndexResponse insertDocumentById(RestHighLevelClient client, String indexName, String typeName, String documentJson,String documentId) throws ElasticSearchException {

        IndexRequest indexRequest = new IndexRequest(indexName, typeName, documentId).source(documentJson, XContentType.JSON);
        String format =  String.format("Method : insertDocumentById , Query details : %s ",indexRequest.getDescription());
        logger.info(format);

        IndexResponse  response = null;
        try {
            response = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to insert the document : "+ documentJson+ " into the index + " + indexName,e);
        }
        return response;
    }
    /**
     * <p> Delete the document by document id from a specific type and index .</p>
     * @return DeleteResponse
     * */
    public static DeleteResponse deleteDocumentById(RestHighLevelClient client, String indexName, String typeName,String documentId) throws ElasticSearchException {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, documentId);

        String format =  String.format("Method : deleteDocumentById , Query details : %s ",deleteRequest.getDescription());
        logger.info(format);

        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("");
            throw new ElasticSearchException("Failed to delete the document : "+ documentId+ " from the index + " + indexName,e);
        }
        return deleteResponse;
    }
    /**
     * <p> Get the document by the document id from a specific type and index. </p>
     * @return GetResponse
     * */
    public static GetResponse getDocumentById(RestHighLevelClient client, String indexName, String typeName,String documentId) throws ElasticSearchException {
        GetRequest getRequest = new GetRequest(indexName,typeName, documentId);
        String format =  String.format("Method : getDocumentById , Query details : %s ",getRequest.getDescription());
        logger.info(format);
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);/**/
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to get the document : "+ documentId+ " from the index + " + indexName,e);
        }
        return getResponse ;
    }
    /**
     * <p> Update the document from a specific type and index by sending the document id and the new document (json).</p>
     * @return UpdateResponse
     * */
    public static UpdateResponse updateDocumentById(RestHighLevelClient client, String indexName, String typeName,String documentJson,String documentId) throws ElasticSearchException {
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, documentId).fetchSource(true);
        updateRequest.doc(documentJson, XContentType.JSON);
        String format =  String.format("Method : updateDocumentById , Query details : %s ",updateRequest.getDescription());
        logger.info(format);

        UpdateResponse updateResponse = null;
        try {
            updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to update the document : "+ documentId+ " from the index + " + indexName,e);
        }
        return updateResponse;
    }

    /**
     * <p> Delete a specific index .</p>
     * @return DeleteResponse
     * */
    public static DeleteResponse deleteIndex(RestHighLevelClient client, String indexName) throws ElasticSearchException {
        DeleteRequest deleteRequest = new DeleteRequest(indexName);

        String format =  String.format("Method : deleteIndex , Query details : %s ",deleteRequest.getDescription());
        logger.info(format);

        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("");
            throw new ElasticSearchException("Failed to delete the index + " + indexName,e);
        }
        return deleteResponse;
    }
    /**
     * <p> Search the documents based on matching field value by using the match query.</p>
     * @return SearchResponse
     * */
    public static SearchResponse getMatchedDocuments(RestHighLevelClient client, String indexName, SearchingCriteria searchCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = prepareMatchQueryBuilder(searchCriteria);
        searchSourceBuilder.query(matchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        String format =  String.format("Method : getMatchedDocuments , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to search the documents from the index + " + indexName,e);
        }
        return response;
    }

    private static MatchQueryBuilder prepareMatchQueryBuilder(SearchingCriteria searchCriteria) {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(searchCriteria.getFieldName(), searchCriteria.getFieldValue());

        matchQueryBuilder.analyzer(searchCriteria.getAnalyzer());
        matchQueryBuilder.boost(searchCriteria.getBoost());
        matchQueryBuilder.minimumShouldMatch(searchCriteria.getMinimumShouldMatch());
        matchQueryBuilder.operator(searchCriteria.getOperator());
        matchQueryBuilder.fuzziness(searchCriteria.getFuzziness());
        matchQueryBuilder.fuzzyTranspositions(searchCriteria.isFuzzyTranspositions());
        matchQueryBuilder.zeroTermsQuery(searchCriteria.getZeroTermsQuery());
        matchQueryBuilder.autoGenerateSynonymsPhraseQuery(searchCriteria.isAutoGenerateSynonymsPhraseQuery());
        matchQueryBuilder.maxExpansions(searchCriteria.getMaxExpansion());
        matchQueryBuilder.prefixLength(searchCriteria.getPrefixLength());
        return matchQueryBuilder;
    }

    /**
     * <p> Search the documents based on matching phrase field value by using the match phrase query.</p>
     * @return SearchResponse
     * */
    public static SearchResponse getMatchedPhraseDocuments(RestHighLevelClient client, String indexName, SearchingCriteria searchCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchPhraseQueryBuilder matchPhraseQueryBuilder = new MatchPhraseQueryBuilder(searchCriteria.getFieldName(),searchCriteria.getFieldValue());
        matchPhraseQueryBuilder.slop(searchCriteria.getSlop());
        matchPhraseQueryBuilder.analyzer(searchCriteria.getAnalyzer());
        matchPhraseQueryBuilder.boost(searchCriteria.getBoost());
        matchPhraseQueryBuilder.zeroTermsQuery(searchCriteria.getZeroTermsQuery());
        searchSourceBuilder.query(matchPhraseQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        String format =  String.format("Method : getMatchedDocumentsByField , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to search the documents from the index + " + indexName,e);
        }
        return response;

    }
    /**
     * <p> Search the documents that contain the words of a provided text, in the same order as provided by using match phrase prefix query.</p>
     * @return SearchResponse
     * */
    public static SearchResponse getMatchedPhrasePrefixDocuments(RestHighLevelClient client, String indexName, SearchingCriteria searchCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = new MatchPhrasePrefixQueryBuilder(searchCriteria.getFieldName(),searchCriteria.getFieldValue());
        matchPhrasePrefixQueryBuilder.slop(searchCriteria.getSlop());
        matchPhrasePrefixQueryBuilder.analyzer(searchCriteria.getAnalyzer());
        matchPhrasePrefixQueryBuilder.maxExpansions(searchCriteria.getMaxExpansion());
        matchPhrasePrefixQueryBuilder.boost(searchCriteria.getBoost());
        searchSourceBuilder.query(matchPhrasePrefixQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        String format =  String.format("Method : getMatchedPhrasePrefixDocuments , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to search the documents from the index + " + indexName,e);
        }
        return response;
    }
    /**
     * <p>Search the documents based on the multi_match query to allow searching on multi-field queries </p>
     * <p>Note : This method supports only the multi_match type criterion</p>
     * @return SearchResponse
     * */
    public static SearchResponse getMatchedDocuments(RestHighLevelClient client, String indexName,String term, String[] fieldNames,SearchingCriteria searchCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(term,fieldNames);
        multiMatchQueryBuilder.type(searchCriteria.getMultiMatchQueryType());
        multiMatchQueryBuilder.analyzer(searchCriteria.getAnalyzer());
        multiMatchQueryBuilder.autoGenerateSynonymsPhraseQuery(searchCriteria.isAutoGenerateSynonymsPhraseQuery());
        multiMatchQueryBuilder.operator(searchCriteria.getOperator());
        multiMatchQueryBuilder.fuzziness(searchCriteria.getFuzziness());
        multiMatchQueryBuilder.maxExpansions(searchCriteria.getMaxExpansion());
        multiMatchQueryBuilder.prefixLength(searchCriteria.getPrefixLength());
        multiMatchQueryBuilder.slop(searchCriteria.getSlop());
        multiMatchQueryBuilder.zeroTermsQuery(searchCriteria.getZeroTermsQuery());
        multiMatchQueryBuilder.fuzzyTranspositions(searchCriteria.isFuzzyTranspositions());
        searchSourceBuilder.query(multiMatchQueryBuilder);

        String format =  String.format("Method : getMatchedDocuments , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to search the documents from the index + " + indexName,e);
        }
        return response;
    }
    /**
     * <p>Search to return the documents that contain terms matching a wildcard pattern </p>
     * <p>Note : This method supports only on the wildcard field with boosting value</p>
     * @return SearchResponse
     * */
    public static SearchResponse getMatchedWildCardDocuments(RestHighLevelClient client, String indexName,SearchingCriteria searchCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        WildcardQueryBuilder wildBuilder = new WildcardQueryBuilder(searchCriteria.getFieldName(),(String)searchCriteria.getFieldValue());
        wildBuilder.boost(searchCriteria.getBoost());
        searchSourceBuilder.query(wildBuilder);
        String format =  String.format("Method : getMatchedWildCardDocuments , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to search the documents from the index + " + indexName,e);
        }
        return response;
    }

    /**
     * <p>Search to return the documents that matches boolean combinations of term queries</p>
     * <p>Note : This method supports only the term boolean combinations with minimumShouldMatch criteria</p>
     * @return SearchResponse
     * */
    public static SearchResponse getMatchedBooleanClausesDocuments(RestHighLevelClient client, String indexName, BoolSearchingCriteria searchCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        searchCriteria.getBoolClauseCriteria().keySet().stream().forEach(key -> applyBoolQuery(key,searchCriteria.getBoolClauseCriteria().get(key),boolQuery));
        searchSourceBuilder.query(boolQuery);

        String format =  String.format("Method : getMatchedBooleanClausesDocuments , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to search the documents from the index + " + indexName,e);
        }

        return response;
    }
    private static void applyBoolQuery(BoolQueryClause key, SearchingCriteria boolCriteria, BoolQueryBuilder boolQuery) {
        switch (key){
            case MUST:
                applyMustClause(boolQuery,boolCriteria);
                break;
            case SHOULD:
                applyShouldClause(boolQuery,boolCriteria);
                break;
            case MUST_NOT:
                applyMustNotClause(boolQuery,boolCriteria);
                break;
            case FILTER:
                applyFilterClause(boolQuery,boolCriteria);
                break;
            default:
                throw new IllegalArgumentException("The bool clause is not defined well");
        }
    }
    private static void applyMustClause(BoolQueryBuilder boolQuery,SearchingCriteria mustCriteria) {
        boolQuery.must(prepareMatchQueryBuilder(mustCriteria));
    }
    private static void applyShouldClause(BoolQueryBuilder boolQuery, SearchingCriteria shouldCriteria) {
        boolQuery.should(prepareMatchQueryBuilder(shouldCriteria));
    }
    private static void applyMustNotClause(BoolQueryBuilder boolQuery, SearchingCriteria mustNotCriteria) {
        boolQuery.mustNot(prepareMatchQueryBuilder(mustNotCriteria));
    }
    private static void applyFilterClause(BoolQueryBuilder boolQuery, SearchingCriteria filterCriteria) {
        boolQuery.filter(prepareMatchQueryBuilder(filterCriteria));
    }

    /**
     * <p>Search to return the documents that have terms within a certain range </p>
     * @return SearchResponse
     * */
    public static SearchResponse getMatchedRangeDocuments(RestHighLevelClient client, String indexName,SearchingCriteria searchCriteria) throws ElasticSearchException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeBuilder = new RangeQueryBuilder(searchCriteria.getFieldName());

        rangeBuilder.gte(searchCriteria.getRangeCriteria().getGte());
        rangeBuilder.lte(searchCriteria.getRangeCriteria().getLt());
        rangeBuilder.relation(searchCriteria.getRangeCriteria().getRelation().toString());
        rangeBuilder.boost(searchCriteria.getBoost());
        rangeBuilder.includeLower(searchCriteria.getRangeCriteria().isIncludeLower());
        rangeBuilder.includeUpper(searchCriteria.getRangeCriteria().isIncludeUpper());

        String rangeFormat = searchCriteria.getRangeCriteria().getFormat();
        if(!util.isVoid(rangeFormat)) {
            rangeBuilder.format(rangeFormat);

        }
        String timeZone = searchCriteria.getRangeCriteria().getTimeZone();
        if(!util.isVoid(timeZone)) {
            rangeBuilder.timeZone(timeZone);
        }
        searchSourceBuilder.query(rangeBuilder);
        String format =  String.format("Method : getMatchedRangeDocuments , Query : %s indexName : %s",searchSourceBuilder,indexName);
        logger.info(format);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new ElasticSearchException("Failed to search the documents from the index + " + indexName,e);
        }

        return response;
    }
}
