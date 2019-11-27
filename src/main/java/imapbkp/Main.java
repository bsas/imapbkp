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
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

import javax.mail.FetchProfile;
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
import org.apache.log4j.Logger;

import com.sun.mail.util.MailSSLSocketFactory;

public class Main {

   final static Logger LOGGER = Logger.getLogger("imapbkp");
   final static int FETCH_SIZE = 10;

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
         LOGGER.error(e.getMessage(), e);
      }

      // TODO: exit error codes???
      System.exit(0);
   }

   private static void openFolder(final Folder folder, final String outputFolder) {
      try {
         for (Folder children : folder.list()) {
            openFolder(children, outputFolder);
         }

         if ((folder.getType() & Folder.HOLDS_MESSAGES) != 0) {
            final String folderName = folder.getFullName().trim().replaceAll("[^A-Za-z0-9]", "");

            final int total = folder.getMessageCount();

            LOGGER.info("Reading label: " + folderName + " (" + total + ")");
            folder.open(Folder.READ_ONLY);

            final Path labelsPath = Paths.get(outputFolder, "labels");
            labelsPath.toFile().mkdirs();
            final Path labelPath = Paths.get(labelsPath.toString(), folderName + ".txt");
            LOGGER.info("Label backup file: " + labelPath.toString());

            // Retrieve messages
            try (FileWriter writer = new FileWriter(labelPath.toFile())) {
               writer.write(folder.getFullName() + ":" + System.lineSeparator());

               for (int i = 1; i <= total; i += FETCH_SIZE) {
                  // Retrieve base info for messages
                  final Message[] messages = folder.getMessages(i, i + FETCH_SIZE);
                  final FetchProfile fp = new FetchProfile();
                  fp.add(FetchProfile.Item.ENVELOPE);
                  fp.add("Message-ID");
                  folder.fetch(messages, fp);

                  // Verify each message
                  for (int count = i; count < (i + FETCH_SIZE) && count <= total; count++) {
                     final Message message = messages[count - i];

                     final String messageFile = saveMessage(message, outputFolder, total, count);
                     LOGGER.debug("Message backup file (" + count + "/" + total + "): " + messageFile);

                     writer.write(messageFile + System.lineSeparator());
                     if (count % FETCH_SIZE == 0) {
                        LOGGER.info("Messages backup (" + count + "/" + total + ")");
                        writer.flush();
                     }
                  }
               }
            }
         }
      } catch (Exception e) {
         LOGGER.error("Error processing folder '" + folder.getFullName(), e);
      } finally {
         if (folder.isOpen()) {
            try {
               folder.close(false);
            } catch (Exception e) {
            }
         }
      }
   }

   private static String saveMessage(final Message message, final String outputFolder, final int total, final int count) throws IOException, MessagingException, NoSuchAlgorithmException {
      String messageFilename = null;
      try (ByteArrayOutputStream data = new ByteArrayOutputStream()) {
         // Get message date fields
         final Calendar sent = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
         sent.setTime(message.getReceivedDate());
         final String day = "" + sent.get(Calendar.DAY_OF_MONTH);
         final String month = "" + (sent.get(Calendar.MONTH) + 1);
         final String year = "" + (sent.get(Calendar.YEAR));

         // Create message ID
         final String[] messageIds = message.getHeader("Message-ID");
         String messageId = null;
         if (messageIds == null || messageIds.length == 0) {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            messageId = new BigInteger(1, digest.digest((message.getSubject() + " : " + sent.getTimeInMillis()).getBytes())).toString(16);
         } else {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            messageId = new BigInteger(1, digest.digest(String.join(",", messageIds).getBytes())).toString(16);
         }

         // Build saving paths
         final Path dataPath = Paths.get(outputFolder, "data", year, month, day);
         final Path messagePath = Paths.get(dataPath.toString(), messageId + ".eml.gz");
         messageFilename = messagePath.toString();
         final File messageFile = messagePath.toFile();

         if (messageFile.exists()) {
            // TODO: Verify SHA-256???
            LOGGER.debug("Message already downloaded (" + count + "/" + total + "): " + messageFilename);
         } else {
            dataPath.toFile().mkdirs();
            LOGGER.debug("Downloading message (" + count + "/" + total + "): " + messageId + "...");
            message.writeTo(data);
            try (OutputStream gzfile = new GZIPOutputStream(new FileOutputStream(messageFile))) {
               data.writeTo(gzfile);
            }

            try (FileWriter writer = new FileWriter(Paths.get(dataPath.toString(), messageId + ".hash").toFile())) {
               final MessageDigest digest = MessageDigest.getInstance("SHA-256");
               writer.write(new BigInteger(1, digest.digest(data.toByteArray())).toString(16));
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
         LOGGER.info("Connection successful: " + store.getURLName());
         openFolder(store.getDefaultFolder(), outputFolder);
      } finally {
         store.close();
         LOGGER.info("Disconnected!");
      }
   }

}
