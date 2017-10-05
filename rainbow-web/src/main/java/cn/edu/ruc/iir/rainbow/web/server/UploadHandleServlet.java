package cn.edu.ruc.iir.rainbow.web.server;

import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class UploadHandleServlet extends HttpServlet {
    public List<String> names;
    public List<FileItem> fileList;

    public UploadHandleServlet() {
        this.names = new ArrayList<String>();
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            DiskFileItemFactory factory = new DiskFileItemFactory();
            ServletFileUpload upload = new ServletFileUpload(factory);
            upload.setHeaderEncoding("UTF-8");
            if (!ServletFileUpload.isMultipartContent(request)) {
                return;
            }
            this.fileList = upload.parseRequest(request);
//            System.out.println(fileList.size());
            for (FileItem item : fileList) {
                if (item.isFormField()) {
                    String value = item.getString("UTF-8");
//                  System.out.print(value);
                    names.add(value);
                } else {
                    String filename = item.getName();
                    if (filename == null) continue;
                    File newFile = new File(filename);
//            		System.out.println(newFile.getCanonicalPath());
//            		item.write(newFile);
                    this.names.add(newFile.getAbsolutePath().toString());
                    saveFile(item);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveFile(FileItem item) {
        InputStream in = null;
        FileOutputStream out = null;
        String realSavePath = null;
        int len = 0;
        byte buffer[] = new byte[1024];
        try {
            in = item.getInputStream();
            if (this.fileList.size() == 10) {
                realSavePath = SysConfig.Catalog_Project + "\\pipeline\\" + fileList.get(0).getString("UTF-8");
                File file = new File(realSavePath);
                if (!file.exists()) {
                    file.mkdirs();
                }
                out = new FileOutputStream(
                        realSavePath + "\\schema.txt", true);
            } else if (this.fileList.size() == 2) {
                realSavePath = SysConfig.Catalog_Project + "\\pipeline\\" + fileList.get(1).getString("UTF-8");
                File file = new File(realSavePath + "\\workload.txt");
                out = new FileOutputStream(file, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                while ((len = in.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
                }
                in.close();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }

    public List<String> upload(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
        return this.names;
    }
}