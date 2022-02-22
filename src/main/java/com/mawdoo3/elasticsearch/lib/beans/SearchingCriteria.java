package com.mawdoo3.elasticsearch.lib.beans;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.search.MatchQuery;

public class SearchingCriteria {

    private String fieldName;
    private Object fieldValue;
    private float boost = 1.0F;
    private String analyzer ;
    private int slop = 0;
    private int maxExpansion = 50;
    private String minimumShouldMatch;
    private Operator operator = Operator.OR;
    private boolean wildCardCaseSensitive = false;
    private Object fuzziness = "0";
    private boolean fuzzyTranspositions = true;
    private RangeCriteria rangeCriteria;
    private boolean autoGenerateSynonymsPhraseQuery = true;
    private int prefixLength = 0;
    private MatchQuery.ZeroTermsQuery zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
    private MultiMatchQueryBuilder.Type multiMatchQueryType = MultiMatchQueryBuilder.Type.BEST_FIELDS;

    public String getFieldName() {
        return fieldName;
    }

    public SearchingCriteria fieldName(String fieldName) {
        if(fieldName == null){
            throw new IllegalArgumentException("field name can't be null");
        }else {
            this.fieldName = fieldName;
        }
        return this;
    }

    public Object getFieldValue() {
        return fieldValue;
    }

    public SearchingCriteria fieldValue(Object fieldValue) {
        this.fieldValue = fieldValue;
        return this;
    }

    public float getBoost() {
        return this.boost;
    }

    public SearchingCriteria boost(float boost) {
        this.boost = boost;
        return this;
    }

    public String getAnalyzer() {
        return this.analyzer;
    }

    public SearchingCriteria analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public int getSlop() {
        return this.slop;
    }

    public SearchingCriteria slop(int slop) {
        if (slop < 0) {
            throw new IllegalArgumentException("No negative slop allowed.");
        } else {
            this.slop = slop;
            return this;
        }
    }

    public int getMaxExpansion() {
        return this.maxExpansion;
    }

    public SearchingCriteria maxExpansion(int maxExpansion) {
        if (maxExpansion < 0) {
            throw new IllegalArgumentException("No negative maxExpansions allowed.");
        } else {
            this.maxExpansion = maxExpansion;
            return this;
        }
    }
    public String getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public SearchingCriteria minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }
    public Operator getOperator() {
        return operator;
    }

    public SearchingCriteria operator(Operator operator) {
        this.operator = operator;
        return this;
    }

    public MultiMatchQueryBuilder.Type getMultiMatchQueryType() {
        return multiMatchQueryType;
    }

    public SearchingCriteria setMultiMatchQueryType(MultiMatchQueryBuilder.Type multiMatchQueryTYpe) {
        this.multiMatchQueryType = multiMatchQueryTYpe;
        return this;
    }
    public boolean isWildCardCaseSensitive() {
        return wildCardCaseSensitive;
    }

    public SearchingCriteria setWildCardCaseSensitive(boolean wildCardCaseSensitive) {
        this.wildCardCaseSensitive = wildCardCaseSensitive;
        return this;
    }

    public RangeCriteria getRangeCriteria() {
        return rangeCriteria;
    }

    public SearchingCriteria setRangeCriteria(RangeCriteria rangeCriteria) {
        this.rangeCriteria = rangeCriteria;
        return this;
    }

    public Object getFuzziness() {
        return fuzziness;
    }

    public SearchingCriteria fuzziness(Object fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    public boolean isFuzzyTranspositions() {
        return fuzzyTranspositions;
    }

    public SearchingCriteria fuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
        return this;
    }

    public boolean isAutoGenerateSynonymsPhraseQuery() {
        return autoGenerateSynonymsPhraseQuery;
    }

    public SearchingCriteria autoGenerateSynonymsPhraseQuery(boolean autoGenerateSynonymsPhraseQuery) {
        this.autoGenerateSynonymsPhraseQuery = autoGenerateSynonymsPhraseQuery;
        return this;
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public SearchingCriteria prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public MatchQuery.ZeroTermsQuery getZeroTermsQuery() {
        return zeroTermsQuery;
    }

    public SearchingCriteria zeroTermsQuery(MatchQuery.ZeroTermsQuery zeroTermsQuery) {
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }
}
