package org.apache.atlas.model.instance;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class DirectSearchRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private SearchType searchType;

    // Required for SIMPLE search and PIT_CREATE
    private String indexName;

    // Required for SIMPLE and PIT_SEARCH
    private String query;  // Elasticsearch query DSL as string

    // Required for PIT operations (PIT_SEARCH, PIT_DELETE)
    private String pitId;

    // Optional for PIT operations (PIT_CREATE, PIT_SEARCH)
    private Long keepAlive;  // in milliseconds

    public SearchType getSearchType() {
        return searchType;
    }

    public void setSearchType(SearchType searchType) {
        this.searchType = searchType;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getPitId() {
        return pitId;
    }

    public void setPitId(String pitId) {
        this.pitId = pitId;
    }

    public Long getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(Long keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DirectSearchRequest{");
        sb.append("searchType=").append(searchType);
        sb.append(", indexName='").append(indexName).append('\'');
        sb.append(", query='").append(query).append('\'');
        sb.append(", pitId='").append(pitId).append('\'');
        sb.append(", keepAlive=").append(keepAlive);
        sb.append('}');
        return sb.toString();
    }
}





