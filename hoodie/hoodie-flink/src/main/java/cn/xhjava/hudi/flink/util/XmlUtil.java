package cn.xhjava.hudi.flink.util;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2021-06-09
 */
public class XmlUtil {
    public static void main(String arge[]) {
        String xml_1 = "E:\\conf\\maser\\core-site.xml";
        try {
            File f = new File(xml_1);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(f);
            NodeList nl = doc.getElementsByTagName("property");
            for (int i = 0; i < nl.getLength(); i++) {
                String name = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                System.out.println(name + "--" + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public Map<String, String> parseXmlFile(String filePath) {
        Map<String, String> result = new HashMap<>();
        try {
            File f = new File(filePath);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(f);
            NodeList nl = doc.getElementsByTagName("property");
            for (int i = 0; i < nl.getLength(); i++) {
                String name = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                result.put(name, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

