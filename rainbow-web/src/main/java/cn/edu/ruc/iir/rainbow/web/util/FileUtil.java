package cn.edu.ruc.iir.rainbow.web.util;

import java.io.*;
import java.util.Date;

public class FileUtil {
    /**
     * @param file
     * @return String
     * @Title: readFile
     * @Description: 文件的读和写
     */
    public static String readFile(File file) {
        if (!file.exists()) {
            return "";
        }
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            // 获取磁盘的文件
            // File file = new File(fileName);
            // 开始读取磁盘的文件
            fileInputStream = new FileInputStream(file);
            // 创建一个字节流
            inputStreamReader = new InputStreamReader(fileInputStream);
            // 创建一个字节的缓冲流
            bufferedReader = new BufferedReader(inputStreamReader);
            StringBuffer buffer = new StringBuffer();
            String string = null;
            while ((string = bufferedReader.readLine()) != null) {
                buffer.append(string + "\n");
            }
            return buffer.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                bufferedReader.close();
                inputStreamReader.close();
                fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param fileName
     * @return String
     * @Title: readFile
     * @Description:方法的重载
     */
    public static String readFile(String fileName) {
        return readFile(new File(fileName));
    }

    public static void writeFile(String content, String filename)
            throws IOException {
        // 要写入的文件
        File file = new File(filename);
        // 写入流对象
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(file);
            printWriter.print(content);
            printWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (printWriter != null) {
                try {
                    printWriter.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
    }


    public static void writeFile(String content, String filename, boolean flag)
            throws IOException {
        File file = new File(filename);
        FileWriter fw = new FileWriter(file, flag);
        // 写入流对象
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(fw);
            printWriter.print(content);
            printWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (printWriter != null) {
                try {
                    printWriter.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    public static void appendFile(String content, String filename)
            throws IOException {
        boolean flag = false;
        // 要写入的文件
        File file = new File(filename);
        if (file.exists()) {
            flag = true;
        }
        FileWriter fw = new FileWriter(file, true);
        // 写入流对象
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(fw);
            if (flag) {
                printWriter.print("\r\n");
            }
            printWriter.print(content);
            printWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (printWriter != null) {
                try {
                    printWriter.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    /**
     * @param args void
     * @throws IOException
     * @Title: main
     * @Description: 入口函数
     */
    public static void main(String[] args) throws IOException {
        String aCashe = new Date().toString();
        FileUtil fileUtil = new FileUtil();
        fileUtil.write(aCashe);
    }

    private void write(String aCashe) throws IOException {
        File file = new File(this.getClass().getClassLoader()
                .getResource(("cashe/cashe.txt")).getFile());
        String filename = file.getAbsolutePath();
        filename = filename.replace("cashe.txt", "20170518205458.txt");
        // filename = filename.replace("cashe.txt", DateUtil.mkTime(new Date())
        // + ".txt");
        System.out.println(filename);
        FileUtil.appendFile(aCashe, filename);
    }

    private static void FileFunc() throws IOException {
        // 公式：内容+模板=文件
        String pack = "com.hh.server";
        String model = "server";
        String rootPath = "E:/JSP Project/minjieshi/";
        String srcPath = rootPath + "src/template/entity.txt";
        System.out.println("1. " + srcPath);
        // 获取模板的内容
        String templateContent = readFile(srcPath);
        templateContent = templateContent.replaceAll("\\[package\\]", pack)
                .replaceAll("\\[model\\]", model);
        // 将替换的内容写入到工程的目录下面
        String path = pack.replaceAll("\\.", "/");
        System.out.println("2. " + path);
        String filePath = rootPath + "src/" + path;
        System.out.println("3. " + filePath);
        File rootFile = new File(filePath);
        if (!rootFile.exists()) {
            rootFile.mkdirs();
        }
        String fileName = filePath + "/" + model + ".java";
        System.out.println("4. " + fileName);
        writeFile(templateContent, fileName);
    }

    public static void delDirectory(String path) {
        File f = new File(path);
        delDirectory(f);
    }

    public static void delDirectory(File path) {
        if (!path.exists())
            return;
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (int i = 0; i < files.length; i++) {
            delDirectory(files[i]);
        }
        path.delete();
    }
}
