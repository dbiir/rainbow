package cn.edu.ruc.iir.rainbow.common.util;

/**
 * Created by hank on 1/28/2015.
 */

import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLException;
import java.io.*;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HttpFactory
{
    private static HttpFactory instance = null;

    private PoolingClientConnectionManager cm = null;
    private List<String> userAgents = null;
    private Random rand = null;

    private HttpFactory()
    {
        try
        {
            int pooledHttpConn = 10;
            if (pooledHttpConn < 24)
            {
                pooledHttpConn = 24;
            }
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
            schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));

            cm = new PoolingClientConnectionManager(schemeRegistry);
            // Increase max total connection to 200
            cm.setMaxTotal(pooledHttpConn);
            // Increase default max connection per route to 20
            cm.setDefaultMaxPerRoute(2);
            // Increase max connections for localhost:80 to 50
            //HttpHost googleResearch = new HttpHost("research.google.com", 80);
            //HttpHost wikipediaEn = new HttpHost("en.wikipedia.org", 80);
            //cm.setMaxPerRoute(new HttpRoute(googleResearch), pooledHttpConn / 6);
            //cm.setMaxPerRoute(new HttpRoute(wikipediaEn), pooledHttpConn / 2);
            userAgents = new ArrayList<String>();
            rand = new Random(System.currentTimeMillis());
            userAgents
                    .add("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.160 Safari/537.22");// linux
            // chrome25
            userAgents
                    .add("Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.160 Safari/537.22");// linux
            // chrome
            userAgents
                    .add("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.172 Safari/537.22");// win7
            // chrome
            userAgents.add("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:8.0) Gecko/20100101 Firefox/8.0");// win7
            // firefox
            userAgents.add("Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)");// win7
            // ie9
            userAgents.add("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)");// win7
            // ie10
            userAgents.add("Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)");// win7
            // ie8
            userAgents.add("Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 5.1; Trident/5.0)");// xp
            // ie9
            userAgents.add("Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)");// xp
            // ie8
            userAgents
                    .add("Mozilla/5.0 (X11; U; Linux i686; en-US) AppleWebKit/534.15 (KHTML, like Gecko) Ubuntu/10.10 Chromium/10.0.613.0 Chrome/10.0.613.0 Safari/534.15");// ubuntu11
            // chrome10
            userAgents
                    .add("Mozilla/5.0 (X11; U; Linux x86_64; en-US) AppleWebKit/534.15 (KHTML, like Gecko) Chrome/10.0.613.0 Safari/534.15");
            userAgents.add("Opera/9.80 (X11; Linux x86_64; U; en) Presto/2.7.62 Version/11.00");
            userAgents.add("Opera/9.80 (X11; Linux i686; U; en) Presto/2.7.62 Version/11.00");
            userAgents.add("Opera/9.80 (Windows NT 6.0; U; en) Presto/2.7.39 Version/11.00");
            userAgents.add("Opera/9.80 (Windows NT 5.1; U; en) Presto/2.7.39 Version/11.00");
            userAgents.add("Mozilla/4.0 (compatible; MSIE 8.0; X11; Linux x86_64; en) Opera 11.00");
            userAgents.add("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; en) Opera 11.00");
            userAgents.add("Opera/9.80 (X11; Linux i686; U; en) Presto/2.5.27 Version/10.60");
            userAgents.add("Mozilla/5.0 (Windows NT 5.1; rv:2.0b9pre) Gecko/20110105 Firefox/4.0b9pre");
            userAgents
                    .add("Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.2) Gecko/20091218 Firefox 3.6b5");
            userAgents.add("Mozilla/5.0 (X11; U; FreeBSD i386; en-US; rv:1.9.2.9) Gecko/20100913 Firefox/3.6.9");
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public HttpClient getHttpClient()
    {
        DefaultHttpClient client = new DefaultHttpClient(cm);
        Integer socketTimeout = 10000;
        Integer connectionTimeout = 10000;
        final int retryTime = 3;
        client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, socketTimeout);
        client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, connectionTimeout);
        client.getParams().setParameter(CoreConnectionPNames.TCP_NODELAY, false);
        client.getParams().setParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 1024 * 1024);
        HttpRequestRetryHandler myRetryHandler = new HttpRequestRetryHandler()
        {
            @Override
            public boolean retryRequest(IOException exception, int executionCount, HttpContext context)
            {
                if (executionCount >= retryTime)
                {
                    // Do not retry if over max retry count
                    return false;
                }
                if (exception instanceof InterruptedIOException)
                {
                    // Timeout
                    return false;
                }
                if (exception instanceof UnknownHostException)
                {
                    // Unknown host
                    return false;
                }
                if (exception instanceof ConnectException)
                {
                    // Connection refused
                    return false;
                }
                if (exception instanceof SSLException)
                {
                    // SSL handshake exception
                    return false;
                }
                HttpRequest request = (HttpRequest) context.getAttribute(ExecutionContext.HTTP_REQUEST);
                boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
                if (idempotent)
                {
                    // Retry if the request is considered idempotent
                    return true;
                }
                return false;
            }

        };
        client.setHttpRequestRetryHandler(myRetryHandler);
        return client;
    }

    public HttpResponse getHttpResponse(String url) throws ClientProtocolException, IOException
    {
        HttpResponse response = null;
        HttpGet get = new HttpGet(url);
        get.addHeader("Accept", "text/html");
        get.addHeader("Accept-Charset", "utf-8");
        get.addHeader("Accept-Encoding", "gzip");
        get.addHeader("Accept-Language", "en-US,en");
        int uai = rand.nextInt() % userAgents.size();
        if (uai < 0)
        {
            uai = -uai;
        }
        get.addHeader("User-Agent", userAgents.get(uai));
        response = getHttpClient().execute(get);
        HttpEntity entity = response.getEntity();
        Header header = entity.getContentEncoding();
        if (header != null)
        {
            HeaderElement[] codecs = header.getElements();
            for (int i = 0; i < codecs.length; i++)
            {
                if (codecs[i].getName().equalsIgnoreCase("gzip"))
                {
                    response.setEntity(new GzipDecompressingEntity(entity));
                }
            }
        }
        return response;

    }

    public HttpEntity getResponseEntity(String url) throws ClientProtocolException, IOException
    {
        HttpResponse response = getHttpResponse(url);
        return response.getEntity();
    }

    /**
     * get the content of html source code from given url
     *
     * @param url
     * @return
     * @throws IOException
     * @throws ClientProtocolException
     */
    public String getPageHtml(String url) throws ClientProtocolException, IOException
    {
        InputStream in = null;
        String page = null;
        try
        {
            HttpEntity entity = getResponseEntity(url);
            in = entity.getContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String newLine = System.getProperty("line.separator");
            StringBuffer buffer = new StringBuffer();
            String line = null;
            while ((line = reader.readLine()) != null)
            {
                buffer.append(line + newLine);
            }
            page = new String(buffer);
        } catch (ClientProtocolException e)
        {
            throw e;
        } catch (IOException e)
        {
            throw e;
        } finally
        {
            if (in != null)
                in.close();
        }
        return page;
    }

    public static HttpFactory getInstance()
    {
        if (instance == null)
        {
            instance = new HttpFactory();
        }
        return instance;
    }
}
