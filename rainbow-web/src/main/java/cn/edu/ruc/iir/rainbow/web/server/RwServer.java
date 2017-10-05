package cn.edu.ruc.iir.rainbow.web.server;


import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RwServer {

    private static Logger log = LoggerFactory.getLogger(RwServer.class);
    private Server server;
    private static int port = 8080;

    public void setPort(int port) {
        this.port = port;
    }

    public void start() throws Exception {
        String relativelyPath = System.getProperty("user.dir");

        server = new Server(port);
        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setWar(relativelyPath + "\\rainbow-web\\target\\rainbow-web.war");
        webAppContext.setParentLoaderPriority(true);
        webAppContext.setServer(server);
        webAppContext.setClassLoader(ClassLoader.getSystemClassLoader());
        webAppContext.getSessionHandler().getSessionManager()
                .setMaxInactiveInterval(10);
        server.setHandler(webAppContext);
        server.start();
    }

    public static void main(String[] args) throws Exception {
        RwServer rwServer = new RwServer();
        rwServer.start();
    }

}
