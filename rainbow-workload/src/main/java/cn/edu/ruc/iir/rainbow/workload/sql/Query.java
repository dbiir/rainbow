package cn.edu.ruc.iir.rainbow.workload.sql;

import java.io.Serializable;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.xspace.workload.sql
 * @ClassName: Query
 * @Description: Get query statistics from url
 * @author: taoyouxian
 * @date: Create in 2018-01-15 23:26
 **/
public class Query implements Serializable {
    private String queryId;
    private String no;
    private String query;

    public Query() {
    }

    public Query(String query, String no, String queryId) {
        this.query = query;
        this.no = no;
        this.queryId = queryId;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
}
