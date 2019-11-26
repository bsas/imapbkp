package imapbkp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.sun.mail.util.MailSSLSocketFactory;

public class Main {

   public static void main(String[] args) {
      // CLI options
      final Options options = new Options();
      options.addOption(new Option("h", "help", false, "Help."));
      options.addOption(new Option("p", "props", true, "Properties file."));
      options.addOption(new Option("o", "output", true, "Output folder."));

      // Create the CLI parser
      final CommandLineParser parser = new DefaultParser();
      try {
         // Parse the command line arguments
         final CommandLine line = parser.parse(options, args);
         if (line.hasOption("help")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("imapbkp", options, true);
         } else {
            final String props = line.hasOption("props") ? line.getOptionValue("props") : "imapbkp.props";
            String output = line.hasOption("output") ? line.getOptionValue("output") : System.getProperty("user.dir");
            run(props, output);
         }
      } catch (Exception e) {
         System.err.println(e);
      }

      // TODO: exit error codes???
      System.exit(0);
   }

   private static void openFolder(Folder folder, String outputFolder) {
      try {
         for (Folder children : folder.list()) {
            openFolder(children, outputFolder);
         }

         if ((folder.getType() & Folder.HOLDS_MESSAGES) != 0) {
            System.out.println("Reading label: " + folder.getFullName());
            folder.open(Folder.READ_ONLY);

            final Set<String> list = new HashSet<String>();
            for (Message message : folder.getMessages()) {
               final String messageFile = saveMessage(message, outputFolder);
               list.add(messageFile);
               System.out.println("Message backup file: " + messageFile);
            }

            final Path labelsPath = Paths.get(outputFolder, "labels");
            labelsPath.toFile().mkdirs();
            final Path labelPath = Paths.get(labelsPath.toString(), folder.getFullName() + ".txt");
            try (FileWriter writer = new FileWriter(labelPath.toFile())) {
               for (String str : list) {
                  writer.write(str + System.lineSeparator());
               }
               System.out.println("Label backup file: " + labelPath.toString());
            }
         }
      } catch (Exception e) {
         System.err.println("Error processing folder '" + folder.getFullName() + "': " + e.getMessage());
      } finally {
         if (folder.isOpen()) {
            try {
               folder.close(false);
            } catch (Exception e) {
            }
         }
      }
   }

   private static String saveMessage(Message message, String outputFolder) throws IOException, MessagingException, NoSuchAlgorithmException {
      String messageFilename = null;
      boolean wasRead = false;
      try (ByteArrayOutputStream data = new ByteArrayOutputStream()) {
         // Create message ID
         final MessageDigest digest = MessageDigest.getInstance("SHA-256");
         final String[] messageIds = message.getHeader("Message-ID");
         String messageId = null;
         if (messageIds == null || messageIds.length == 0) {
            message.writeTo(data);
            wasRead = true;
            messageId = new BigInteger(1, digest.digest(data.toByteArray())).toString(16);
         } else {
            messageId = new BigInteger(1, digest.digest(String.join(",", messageIds).getBytes())).toString(16);
         }

         final Path dataPath = Paths.get(outputFolder, "data", messageId.substring(0, 4));
         final Path messagePath = Paths.get(dataPath.toString(), messageId + ".eml.gz");
         messageFilename = messagePath.toString();
         final File messageFile = messagePath.toFile();

         if (messageFile.exists()) {
            // TODO: Verify SHA-256???
            System.out.println("Message already downloaded: " + messageFilename);
         } else {
            dataPath.toFile().mkdirs();
            System.out.println("Downloading message: " + messageId + "...");
            if (!wasRead) {
               message.writeTo(data);
               wasRead = true;
            }
            try (OutputStream gzfile = new GZIPOutputStream(new FileOutputStream(messageFile))) {
               data.writeTo(gzfile);
            }
         }
      }
      return messageFilename;
   }

   public static void run(String propsFile, String outputFolder) throws GeneralSecurityException, IOException, MessagingException {
      // Read properties
      final Properties props = new Properties();
      try (InputStream in = new FileInputStream(new File(propsFile))) {
         props.load(in);
      }

      // Setup SSL context
      boolean isSSL = "true".equals(props.get("mail.imap.ssl.enable")) || "true".equals(props.get("mail.imap.starttls.enable"));
      if (isSSL) {
         final MailSSLSocketFactory sf = new MailSSLSocketFactory();
         sf.setTrustAllHosts(true);
         props.put("mail.imap.ssl.socketFactory", sf);
      }

      // Setup variables
      final String host = isSSL ? props.getProperty("mail.imaps.host") : props.getProperty("mail.imap.host");
      final String user = isSSL ? props.getProperty("mail.imaps.user") : props.getProperty("mail.imap.user");
      final String password = isSSL ? props.getProperty("mail.imaps.password") : props.getProperty("mail.imap.password");
      final int port = Integer.parseInt(isSSL ? props.getProperty("mail.imaps.port") : props.getProperty("mail.imap.port"));

      // Connect to IMAP server
      Store store = null;
      try {
         final Session session = javax.mail.Session.getInstance(props);
         store = session.getStore(isSSL ? "imaps" : "imap");
         store.connect(host, port, user, password);
         System.out.println("Connection successful: " + store.getURLName());
         openFolder(store.getDefaultFolder(), outputFolder);
      } finally {
         store.close();
         System.out.println("Disconnected!");
      }
   }

}
