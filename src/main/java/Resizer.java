import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.commons.io.FileUtils;
import org.imgscalr.Scalr;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.UUID;

public class Resizer implements RequestHandler<ResizerInput, String> {
    AmazonS3 s3client;
    public String handleRequest(ResizerInput i, Context context) {
        String resizedUrl = createUrl(i, context);
        if(!alreadyExists(resizedUrl)) {
            BufferedImage originalImage = readImage(i, context);
            if(originalImage != null) {
                InputStream resizedImage = resizerImage(originalImage, i, context);
                if(resizedImage != null) {
                    if(!storeImageInS3(resizedImage, resizedUrl, context)) {
                        return "Failed to store Resized Image in S3";
                    } else {
                        return resizedUrl;
                    }
                } else {
                    return "Failed to resize Image";
                }
            } else {
                return "Failed to read Original Image";
            }
        }
        return resizedUrl;
    }
    //create a URL for the s3 object of resized image
    private String createUrl(ResizerInput i, Context context) {
        String resizedUrl = "";
        String publicUrl = System.getenv("publicurl");// the root of bucket
        String fullHash = "" + Math.abs(i.getUrl().hashCode());
        String fileName = "";
        try {
            fileName = Paths.get(new URI("").getPath()).getFileName().toString();
        } catch (URISyntaxException ex) {
            context.getLogger().log("Unable to create url: " + i.getUrl() + " " + ex.getMessage());
        }

        resizedUrl = publicUrl + fileName + "-" + fullHash + "-" + i.getWidth() + "-" + i.getHeight();
        return resizedUrl;

    }
    //use Java Image Io class to read original image into a buffered image that we can use to resize .
    private BufferedImage readImage(ResizerInput i , Context context) {
        try {
            return ImageIO.read(new URL(i.getUrl()).openStream());
        } catch (IOException ex) {
            context.getLogger().log("Failed to read original url: " + i.getUrl() + " " + ex.getMessage());
            return null;
        }
    }
    //use Image Scalr to resize the original image (buffered image) to the desired size, and return the input stream
    //of resized image
    private InputStream resizerImage(BufferedImage image, ResizerInput i, Context context) {
        try {
            BufferedImage img = Scalr.resize(image, Scalr.Method.BALANCED, Scalr.Mode.AUTOMATIC, i.getWidth(), i.getHeight());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            ImageIO.write(img, "gif", os);
            InputStream is = new ByteArrayInputStream(os.toByteArray());
            return is;
        } catch (IOException ex) {
            context.getLogger().log("Image Resizing failed: " + i.getUrl() + " " + ex.getMessage());
            return null;
        }
    }
    //return s3 client object that we can use to talk to S3 service
    private AmazonS3 getS3client(){
        if(s3client == null){
           s3client = AmazonS3Client.builder().withCredentials(DefaultAWSCredentialsProviderChain.getInstance()).build();
        }
        return s3client;
    }
    // get the s3 key from the URL of resized image, return file name section of the URL
    private String getS3Key(String resizedUrl) {
        try {
            return Paths.get(new URI(resizedUrl).getPath()).getFileName().toString();
        } catch (URISyntaxException ex ) {
            return "";
        }
    }

    private boolean storeImageInS3(InputStream is, String resizedUrl, Context context) {
        String s3Key = getS3Key(resizedUrl);
        String bucketName = System.getenv("bucketname");
        File tempFile = null;
        try {
            //create a temp file in the lambda
            tempFile = File.createTempFile(UUID.randomUUID().toString(), ",gif");
            //put the resized image into the file
            FileUtils.copyInputStreamToFile(is, tempFile);
            //make a request to S3 to store  the file, with access permission: public
            context.getLogger().log("Storing in S3 " + bucketName + "/" + s3Key);
            PutObjectRequest por = new PutObjectRequest(bucketName, s3Key, tempFile).withCannedAcl(CannedAccessControlList.PublicRead);
            //complete the request, put the tempFile into S3
            PutObjectResult res = getS3client().putObject(por);
            context.getLogger().log("Stored in S3 " + bucketName + "/" + s3Key);
        } catch (IOException e) {
            context.getLogger().log("Error creating temp file: " + e.getMessage());
            return false;
        } finally {
            // delete the temp file
            if(tempFile != null) {
                tempFile.delete();
            }
        }
        return true;
    }
    // check if the resized image already exists in S3
    private boolean alreadyExists(String resizedUrl) {
        String bucketName = System.getenv("bucketname");
        //check the S3 metadata of the file, if it fails, the file doesn't exist
        try {
            getS3client().getObjectMetadata(bucketName, getS3Key(resizedUrl));
        } catch (AmazonServiceException e) {
            return false;
        }
        return true;
    }
}
