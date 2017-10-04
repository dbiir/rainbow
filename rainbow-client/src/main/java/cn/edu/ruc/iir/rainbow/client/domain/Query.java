package cn.edu.ruc.iir.rainbow.client.domain;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.client.domain
 * @ClassName: Query
 * @Description: contains the parameters of the workload.
 * @author: Tao
 * @date: Create in 2017-09-30 9:03
 **/
public class Query {
    private String query;
    private String pno;
    private String id;


    public Query() {
    }

    public Query(String query, String pno, String id) {
        this.query = query;
        this.pno = pno;
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getPno() {
        return pno;
    }

    public void setPno(String pno) {
        this.pno = pno;
    }
}
