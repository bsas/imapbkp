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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

import javax.mail.FetchProfile;
import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.SearchTerm;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

import com.sun.mail.util.MailSSLSocketFactory;

public class Main {

   private static Logger LOGGER = Logger.getLogger("imapbkp");
   private static int FETCH_SIZE = 10;

   public static void main(String[] args) {
      // CLI options
      final Options options = new Options();
      options.addOption(new Option("h", "help", false, "Help."));
      options.addOption(new Option("p", "props", true, "Properties file."));
      options.addOption(new Option("o", "output", true, "Output folder."));
      options.addOption(new Option("l", "log", true, "Log file."));
      options.addOption(new Option("f", "force", false, "Force download all messages."));
      options.addOption(new Option("d", "date", true, "Force messages from this date."));

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
            final String output = line.hasOption("output") ? line.getOptionValue("output") : System.getProperty("user.dir");

            // Find last date
            Date fromDate = null;
            if (!line.hasOption("force")) {
               final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
               fromDate = line.hasOption("date") ? formatter.parse(line.getOptionValue("date")) : findLastSyncDate(output);
            }
            LOGGER.info(fromDate == null ? "Full sync... (that can take a lot of time)" : "Sync all messages since " + fromDate);

            // Configure log file
            if (line.hasOption("log")) {
               LOGGER.addAppender(new FileAppender(new org.apache.log4j.PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"), line.getOptionValue("log"), false));
            }

            run(props, output, fromDate);
         }
      } catch (Exception e) {
         LOGGER.error(e.getMessage(), e);
      }

      // TODO: exit error codes???
      System.exit(0);
   }

   private static void openFolder(final Folder folder, final String outputFolder, final Date fromDate) {
      try {
         for (Folder children : folder.list()) {
            openFolder(children, outputFolder, fromDate);
         }

         if ((folder.getType() & Folder.HOLDS_MESSAGES) != 0) {
            final String folderName = folder.getFullName().trim().replaceAll("[^A-Za-z0-9]", "");
            folder.open(Folder.READ_ONLY);

            Message[] messages = null;
            int total = 0;
            if (fromDate == null) {
               total = folder.getMessageCount();
            } else {
               final SearchTerm newerThan = new ReceivedDateTerm(ComparisonTerm.GT, fromDate);
               messages = folder.search(newerThan);
               total = messages.length;
            }
            LOGGER.info("Reading label: " + folderName + " (" + total + ")");

            final Path labelsPath = Paths.get(outputFolder, "labels");
            labelsPath.toFile().mkdirs();
            final Path labelPath = Paths.get(labelsPath.toString(), folderName + ".txt");
            LOGGER.info("Label backup file: " + labelPath.toString());

            // Retrieve messages
            try (FileWriter writer = new FileWriter(labelPath.toFile())) {
               writer.write(folder.getFullName() + ":" + System.lineSeparator());

               for (int i = 1; i <= total; i += FETCH_SIZE) {
                  if (fromDate == null) {
                     // Retrieve base info for messages
                     messages = folder.getMessages(i, i + FETCH_SIZE);
                     final FetchProfile fp = new FetchProfile();
                     fp.add(FetchProfile.Item.ENVELOPE);
                     fp.add("Message-ID");
                     folder.fetch(messages, fp);
                  }

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

   private static Date findLastSyncDate(final String outputFolder) throws IOException {
      final File dataPath = Paths.get(outputFolder, "data").toFile();
      final int year = Arrays.asList(dataPath.list()).stream().mapToInt(v -> Integer.parseInt(v)).max().orElse(-1);
      final int month = year == -1 ? -1 : Arrays.asList(Paths.get(dataPath.getAbsolutePath(), "" + year).toFile().list()).stream().mapToInt(v -> Integer.parseInt(v)).max().orElse(-1);
      final int day = month == -1 ? -1 : Arrays.asList(Paths.get(dataPath.getAbsolutePath(), "" + year, "" + month).toFile().list()).stream().mapToInt(v -> Integer.parseInt(v)).max().orElse(-1);

      Date fromDate = null;
      if (year != -1 && month != -1 && day != -1) {
         final Calendar last = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
         last.set(Calendar.YEAR, year);
         last.set(Calendar.MONTH, month - 1);
         last.set(Calendar.DAY_OF_MONTH, day);
         last.set(Calendar.HOUR, 0);
         last.set(Calendar.MINUTE, 0);
         last.set(Calendar.SECOND, 0);
         last.set(Calendar.MILLISECOND, 0);
         last.add(Calendar.DAY_OF_MONTH, -1);
         fromDate = last.getTime();
      }

      return fromDate;
   }

   public static void run(final String propsFile, final String outputFolder, final Date fromDate) throws GeneralSecurityException, IOException, MessagingException {
      // Read properties
      final Properties props = new Properties();
      try (InputStream in = new FileInputStream(new File(propsFile))) {
         props.load(in);
      }
      FETCH_SIZE = Integer.parseInt(props.getProperty("imapbkp.fetchsize", "" + FETCH_SIZE));

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
         openFolder(store.getDefaultFolder(), outputFolder, fromDate);
      } finally {
         store.close();
         LOGGER.info("Disconnected!");
      }
   }

}
