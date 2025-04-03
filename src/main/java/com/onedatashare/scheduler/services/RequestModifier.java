package com.onedatashare.scheduler.services;

import com.onedatashare.scheduler.enums.EndPointType;
import com.onedatashare.scheduler.model.*;
import com.onedatashare.scheduler.model.credential.AccountEndpointCredential;
import com.onedatashare.scheduler.model.credential.OAuthEndpointCredential;
import com.onedatashare.scheduler.services.expanders.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class RequestModifier {

    Logger logger = LoggerFactory.getLogger(RequestModifier.class);

    @Autowired
    CredentialService credentialService;

    @Autowired
    SFTPExpander sftpExpander;

    @Autowired
    FTPExpander ftpExpander;

    @Autowired
    S3Expander s3Expander;

    @Autowired
    BoxExpander boxExpander;

    @Autowired
    DropBoxExpander dropBoxExpander;

    @Autowired
    HttpExpander httpExpander;

    @Autowired
    GDriveExpander gDriveExpander;

    @Autowired
    StaticOptimizer staticOptimizer;

    public static Set<EndPointType> vfsCredType = new HashSet<>(Arrays.asList(new EndPointType[]{EndPointType.s3, EndPointType.ftp, EndPointType.sftp, EndPointType.http, EndPointType.scp}));
    public static Set<EndPointType> oAuthType = new HashSet<>(Arrays.asList(new EndPointType[]{EndPointType.dropbox, EndPointType.box, EndPointType.gdrive}));

    public List<EntityInfo> selectAndExpand(TransferJobRequest.Source source, List<EntityInfo> selectedResources) {
        logger.info("The info list in select and expand is \n" + selectedResources.toString());
        switch (source.getType()) {
            case ftp:
                ftpExpander.createClient(source.getVfsSourceCredential());
                return ftpExpander.expandedFileSystem(selectedResources, source.getFileSourcePath());
            case s3:
                s3Expander.createClient(source.getVfsSourceCredential());
                return s3Expander.expandedFileSystem(selectedResources, source.getFileSourcePath());
            case sftp:
            case scp:
                sftpExpander.createClient(source.getVfsSourceCredential());
                return sftpExpander.expandedFileSystem(selectedResources, source.getFileSourcePath());
            case http:
                httpExpander.createClient(source.getVfsSourceCredential());
                return httpExpander.expandedFileSystem(selectedResources, source.getFileSourcePath());
            case box:
                boxExpander.createClient(source.getOauthSourceCredential());
                return boxExpander.expandedFileSystem(selectedResources, source.getFileSourcePath());
            case dropbox:
                dropBoxExpander.createClient(source.getOauthSourceCredential());
                return dropBoxExpander.expandedFileSystem(selectedResources, source.getFileSourcePath());
            case vfs:
                return selectedResources;
            case gdrive:
                gDriveExpander.createClient(source.getOauthSourceCredential());
                return gDriveExpander.expandedFileSystem(selectedResources, source.getFileSourcePath());

        }
        return null;
    }

    public List<EntityInfo> selectAndExpandDestination(TransferJobRequest.Destination destination, List<EntityInfo> selectedResources) {
        //logger.info("The info list in select and expand is \n" + selectedResources.toString());
        switch (destination.getType()) {
            case ftp:
                ftpExpander.createClient(destination.getVfsDestCredential());
                return ftpExpander.expandedDestFileSystem(destination.getFileDestinationPath());
            case s3:
                s3Expander.createClient(destination.getVfsDestCredential());
                return s3Expander.expandedDestFileSystem(destination.getFileDestinationPath());
            case sftp:
            case scp:
                sftpExpander.createClient(destination.getVfsDestCredential());
                return sftpExpander.expandedDestFileSystem(destination.getFileDestinationPath());
            case http:
                httpExpander.createClient(destination.getVfsDestCredential());
                return httpExpander.expandedDestFileSystem(destination.getFileDestinationPath());
            case box:
                boxExpander.createClient(destination.getOauthDestCredential());
                return boxExpander.expandedDestFileSystem(destination.getFileDestinationPath());
            case dropbox:
                dropBoxExpander.createClient(destination.getOauthDestCredential());
                return dropBoxExpander.expandedDestFileSystem(destination.getFileDestinationPath());
            case vfs:
                return selectedResources;
            case gdrive:
                gDriveExpander.createClient(destination.getOauthDestCredential());
                return gDriveExpander.expandedDestFileSystem(destination.getFileDestinationPath());

        }
        return null;
    }

    /**
     * This method is supposed to take a list of Files that are expanded and make sure the chunk size being used is supported by the source/destination
     * So far all what I can tell is that Box has an "optimized" chunk size that we should use to write too.
     *
     * @param entityInfo
     * @param destination
     * @param userChunkSize
     * @return List of Files that have the proper chunkSize to use during the transfer.
     */
    public List<EntityInfo> checkDestinationChunkSize(List<EntityInfo> entityInfo, TransferJobRequest.Destination destination, Integer userChunkSize) {
        entityInfo.forEach(entityInfo1 -> {
            if (entityInfo1.getChunkSize() == 0) {
                entityInfo1.setChunkSize(userChunkSize);
            }
        });
        switch (destination.getType()) {
            case box:
                boxExpander.createClient(destination.getOauthDestCredential());
                if (destination.getFileDestinationPath() == null || destination.getFileDestinationPath().isEmpty()) {
                    destination.setFileDestinationPath("0");
                }
                return boxExpander.destinationChunkSize(entityInfo, destination.getFileDestinationPath(), userChunkSize);
            case dropbox:
                return dropBoxExpander.destinationChunkSize(entityInfo, destination.getFileDestinationPath(), userChunkSize);
            case ftp:
                return ftpExpander.destinationChunkSize(entityInfo, destination.getFileDestinationPath(), userChunkSize);
            case sftp:
            case scp:
                return sftpExpander.destinationChunkSize(entityInfo, destination.getFileDestinationPath(), userChunkSize);
            case s3:
                return s3Expander.destinationChunkSize(entityInfo, destination.getFileDestinationPath(), userChunkSize);
            case http:
                return httpExpander.destinationChunkSize(entityInfo, destination.getFileDestinationPath(), userChunkSize);
        }

        return entityInfo;
    }

    public TransferJobRequest createRequest(RequestFromODSDTO odsTransferRequest, UUID jobUuid) {
        logger.info(odsTransferRequest.toString());
        TransferJobRequest transferJobRequest = new TransferJobRequest();
        transferJobRequest.setOptions(TransferOptions.createTransferOptionsFromUser(odsTransferRequest.getOptions()));
        transferJobRequest.setOwnerId(odsTransferRequest.getOwnerId());
        transferJobRequest.setJobUuid(jobUuid);
        TransferJobRequest.Source s = new TransferJobRequest.Source();
        s.setCredId(odsTransferRequest.getSource().getCredId());
        s.setFileSourcePath(odsTransferRequest.getSource().getFileSourcePath());
        s.setType(odsTransferRequest.getSource().getType());
        TransferJobRequest.Destination d = new TransferJobRequest.Destination();
        d.setFileDestinationPath(odsTransferRequest.getDestination().getFileDestinationPath());
        d.setCredId(odsTransferRequest.getDestination().getCredId());
        d.setType(odsTransferRequest.getDestination().getType());

        switch (odsTransferRequest.getSource().getType()) {
            case scp, http, ftp, sftp, s3:
                AccountEndpointCredential sourceAccountCred = credentialService.fetchAccountCredential(odsTransferRequest.getSource().getType().toString(), odsTransferRequest.getOwnerId(), odsTransferRequest.getSource().getCredId());
                s.setVfsSourceCredential(sourceAccountCred);
            case gdrive, box, dropbox:
                OAuthEndpointCredential sourceOAuthCred = credentialService.fetchOAuthCredential(odsTransferRequest.getSource().getType(), odsTransferRequest.getOwnerId(), odsTransferRequest.getSource().getCredId());
                s.setOauthSourceCredential(sourceOAuthCred);
        }

        switch (odsTransferRequest.getDestination().getType()) {
            case scp, http, ftp, sftp, s3:
                AccountEndpointCredential destAccountCred = credentialService.fetchAccountCredential(odsTransferRequest.getDestination().getType().toString(), odsTransferRequest.getOwnerId(), odsTransferRequest.getDestination().getCredId());
                d.setVfsDestCredential(destAccountCred);
            case gdrive, box, dropbox:
                OAuthEndpointCredential destOAuthCred = credentialService.fetchOAuthCredential(odsTransferRequest.getDestination().getType(), odsTransferRequest.getOwnerId(), odsTransferRequest.getDestination().getCredId());
                d.setOauthDestCredential(destOAuthCred);
        }

        List<EntityInfo> expandedFiles = this.selectAndExpand(s, odsTransferRequest.getSource().getResourceList());
        logger.info("Expanded files: {}", expandedFiles);
        expandedFiles = this.checkDestinationChunkSize(expandedFiles, d, odsTransferRequest.getOptions().getChunkSize());

        //Overwrite checker
        if (transferJobRequest.getOptions().isOverwrite())
            s.setInfoList(expandedFiles);
        else {
            //Check files present in destination
            List<EntityInfo> destExpandedFiles = this.selectAndExpandDestination(d, new ArrayList<>());

            //for each file ID at dest, check if present in source list, if yes, remove that file id from src info list
            for (EntityInfo destFile : destExpandedFiles) {
                for (EntityInfo srcFile : expandedFiles) {
                    if (destFile.getId().equals(srcFile.getId())) {
                        expandedFiles.remove(srcFile);
                    }
                }
            }
            s.setInfoList(expandedFiles);
        }

        transferJobRequest.setSource(s);
        transferJobRequest.setDestination(d);
        transferJobRequest.setTransferNodeName(odsTransferRequest.getTransferNodeName());
        TransferOptions newOptions = this.staticOptimizer.decideParams(transferJobRequest.getSource().getInfoList(), transferJobRequest.getOptions());
        transferJobRequest.setOptions(newOptions);
        if(odsTransferRequest.getTransferSla() == null) transferJobRequest.setTransferSla(new TransferSla());
        else transferJobRequest.setTransferSla(odsTransferRequest.getTransferSla());

        return transferJobRequest;
    }

    /**
     * Current little hack to make sure writing to S3 results in a chunkSize greater than 5MB if a multipart request.
     * This should be a property that is data mined in the optimization service
     * This method should not be used and is only kept here to support the compatability with an API change where chunkSize was set for the entire transfer and not per file basis.
     *
     * @param destType
     * @param chunkSize
     * @return
     */
    @Deprecated
    public int correctChunkSize(EndPointType destType, int chunkSize) {
        if (destType.equals(EndPointType.s3) && chunkSize < 5000000) { //5MB as we work with bytes not bits!
            return 10000000;
        } else if (destType.equals(EndPointType.gdrive) && chunkSize < 5000000) {
            return 10000000;
        } else {
            return chunkSize;
        }
    }


}
