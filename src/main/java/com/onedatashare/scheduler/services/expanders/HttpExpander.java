package com.onedatashare.scheduler.services.expanders;

import com.onedatashare.scheduler.model.credential.AccountEndpointCredential;
import com.onedatashare.scheduler.model.credential.EndpointCredential;
import com.onedatashare.scheduler.model.EntityInfo;
import com.onedatashare.scheduler.services.DestinationChunkSize;
import com.onedatashare.scheduler.services.FileExpander;
import lombok.SneakyThrows;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

@Component
public class HttpExpander extends DestinationChunkSize implements FileExpander {

    private AccountEndpointCredential credential;
    Logger logger = LoggerFactory.getLogger(HttpExpander.class);

    @Override
    public void createClient(EndpointCredential credential) {
        this.credential = EndpointCredential.getAccountCredential(credential);
        logger.info(this.credential.toString());
    }

    @Override
    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath) {
        List<EntityInfo> filesToSend = new ArrayList<>();
        Stack<Element> directoriesToTraverse = new Stack<>();
        if (basePath.isEmpty()) basePath = "/";
        try {
            if (userSelectedResources.isEmpty()) { //we move the whole damn server
                logger.info("User resources is empty gonna just send the whole server I guess");
                Document doc = Jsoup.connect(this.credential.getUri() + basePath).get();
                Elements links = doc.select("body a");
                for (Element elem : links) {
                    if (elem.text().endsWith("/")) { //directory to expand
                        directoriesToTraverse.push(elem);
                    } else { //we have a file
                        filesToSend.add(fromElement(elem));
                    }
                }
            } else { //move only files/folders the user selected
                for (EntityInfo selectedFiles : userSelectedResources) {
                    //we have a folder to transfer
                    if (selectedFiles.getPath().endsWith("/")) {

                        Document doc = Jsoup.connect(this.credential.getUri() + basePath + selectedFiles.getPath())
                                .ignoreContentType(true)
                                .get();
                        logger.info(doc.toString());
                        Elements links = doc.select("body a");
                        for (Element elem : links) {
                            if (elem.text().endsWith("/")) { //directory to expand
                                directoriesToTraverse.push(elem);
                            } else { //we have a file
                                filesToSend.add(fromElement(elem));
                            }
                        }
                    } else {
                        filesToSend.add(this.fileToInfo(this.credential.getUri() + Paths.get(basePath, selectedFiles.getPath()).toString()));
                    }
                }
            }
            //all of these have names that should be appended to the path
            while (!directoriesToTraverse.isEmpty()) {
                Element directory = directoriesToTraverse.pop();
                if (directory.text().contains("..") || directory.text().contains(".")) {
                    continue;
                }
                logger.info(directory.baseUri() + directory.text());
                Document doc = Jsoup.connect(directory.baseUri() + basePath + directory.text()).get();
                Elements links = doc.select("body a");
                for (Element elem : links) {
                    if (elem.text().endsWith("/")) { //directory to expand
                        directoriesToTraverse.push(elem);
                    } else { //we have a file
                        filesToSend.add(fromElement(elem));
                    }
                }
            }
        } catch (IOException e){
            logger.error("Failed to Expand ip: {} will expand on the TransferNode instead", this.credential.getUri());
            return userSelectedResources;
        }
        return filesToSend;
    }

    @SneakyThrows
    public List<EntityInfo> expandedDestFileSystem(String basePath) {
        List<EntityInfo> destFilesList = new ArrayList<>();
        Stack<Element> directoriesToTraverse = new Stack<>();
        if (basePath.isEmpty()) basePath = "/";

        Document doc = Jsoup.connect(this.credential.getUri() + basePath).get();
        Elements links = doc.select("body a");
        for (Element elem : links) {
            //ignore dir, consider all files and add to list
            if (!elem.text().endsWith("/")) {
                destFilesList.add(fromElement(elem));
            }
        }
        return destFilesList;
    }

    public EntityInfo fromElement(Element elem) throws IOException {
        EntityInfo fileInfo = new EntityInfo();
        URL url = new URL(elem.absUrl("href"));
        long fileSize = url.openConnection().getContentLengthLong();
        Path path = Path.of(url.getPath());
        logger.info("File Name:{}", elem.text());
        logger.info("file size={}", fileSize);
        logger.info("File Path: {}", path);
        fileInfo.setId(elem.text());
        fileInfo.setSize(fileSize);
        fileInfo.setPath(path.toAbsolutePath().toString());
        return fileInfo;
    }

    public EntityInfo fileToInfo(String strUrl) throws IOException {
        EntityInfo fileInfo = new EntityInfo();
        URL url = new URL(strUrl);
        URLConnection conn = url.openConnection();
        long fileSize = conn.getContentLengthLong();
        String fileName = Paths.get(conn.getURL().getFile()).getFileName().toString();
        logger.info("File Name:{}", fileName);
        logger.info("file size={}", fileSize);
        logger.info("File Path: {}", url.getPath());
        fileInfo.setId(fileName);
        fileInfo.setSize(fileSize);
        fileInfo.setPath(url.getPath());
        return fileInfo;
    }
}
