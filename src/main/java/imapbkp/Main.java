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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import javax.mail.Folder;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.search.AndTerm;
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
   private static final String SYNCFILE = "lastupdate.txt";

   public static void main(String[] args) {
      // CLI options
      final Options options = new Options();
      options.addOption(new Option("h", "help", false, "Help."));
      options.addOption(new Option("p", "props", true, "Properties file."));
      options.addOption(new Option("o", "output", true, "Output folder."));
      options.addOption(new Option("l", "log", true, "Log file."));
      options.addOption(new Option("f", "force", false, "Force download all messages."));
      options.addOption(new Option("s", "start", true, "Start date for all downloaded messages."));
      options.addOption(new Option("e", "end", true, "End date for all downloaded messages."));

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
            final boolean force = line.hasOption("force");
            Date fromDate = null;
            Date toDate = null;
            if (!force) {
               final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
               fromDate = line.hasOption("start") ? formatter.parse(line.getOptionValue("start")) : null;
               toDate = line.hasOption("end") ? formatter.parse(line.getOptionValue("end")) : null;
               if (fromDate != null) {
                  LOGGER.info("Sync all messages newer than " + fromDate);
               }
               if (toDate != null) {
                  LOGGER.info("Sync all messages older than " + toDate);
               }
            } else {
               LOGGER.info("Full sync... (that can take a lot of time)");
            }

            // Configure log file
            if (line.hasOption("log")) {
               LOGGER.addAppender(new FileAppender(new org.apache.log4j.PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"), line.getOptionValue("log"), false));
            }

            run(props, output, force, fromDate, toDate);
         }
      } catch (Exception e) {
         LOGGER.error(e.getMessage(), e);
      }

      // TODO: exit error codes???
      System.exit(0);
   }

   private static Map<String, Long> openSyncData(final String outputFolder) throws IOException {
      Map<String, Long> syncData = new HashMap<String, Long>();
      try (final Stream<String> lines = Files.lines(Paths.get(outputFolder, SYNCFILE))) {
         syncData = lines.map(line -> line.split(",")).collect(Collectors.toMap(line -> line[0], line -> Long.parseLong(line[1])));
         LOGGER.info("Sync data loaded: " + syncData);
      } catch (Exception e) {
      }
      return syncData;
   }

   private static void saveSyncData(final String outputFolder, final Map<String, Long> syncData) throws IOException {
      try (FileWriter writer = new FileWriter(Paths.get(outputFolder, SYNCFILE).toFile())) {
         LOGGER.info("Sync data saved: " + syncData);
         final List<String> list = syncData.entrySet().stream().map(e -> e.getKey() + "," + e.getValue()).collect(Collectors.toList());
         writer.write(String.join(System.lineSeparator(), list));
      } catch (Exception e) {
      }
   }

   private static void openFolder(final Folder folder, final String outputFolder, final boolean force, final Date fromDate, final Date toDate, final Map<String, Long> syncData) {
      try {
         for (Folder children : folder.list()) {
            openFolder(children, outputFolder, force, fromDate, toDate, syncData);
         }

         if ((folder.getType() & Folder.HOLDS_MESSAGES) != 0) {
            final String folderName = folder.getFullName().trim().replaceAll("[^A-Za-z0-9]", "");
            folder.open(Folder.READ_ONLY);

            // Retrieve sync date
            final Long lastSync = syncData.get(folderName);
            final Date syncDate = lastSync == null ? new Date(0) : new Date(lastSync - 1);
            if (lastSync == null) {
               syncData.put(folderName, syncDate.getTime());
            }

            // Retrieve messages
            Message[] messages = null;
            int total = 0;
            if (force) {
               messages = folder.getMessages();
               total = folder.getMessageCount();
            } else {
               final SearchTerm newerThan = fromDate == null ? new ReceivedDateTerm(ComparisonTerm.GT, syncDate) : new ReceivedDateTerm(ComparisonTerm.GT, fromDate);
               final SearchTerm olderThan = toDate == null ? null : new ReceivedDateTerm(ComparisonTerm.LT, toDate);
               final SearchTerm term = olderThan == null ? newerThan : new AndTerm(newerThan, olderThan);
               messages = folder.search(term);
               total = messages.length;
            }
            LOGGER.info("Reading label: " + folderName + " (" + total + ")");

            final Path labelsPath = Paths.get(outputFolder, "labels");
            labelsPath.toFile().mkdirs();
            final Path labelPath = Paths.get(labelsPath.toString(), folderName + ".txt");
            LOGGER.info("Label backup file: " + labelPath.toString());

            // Read old file (if it exists)
            final Set<String> list = new HashSet<String>();
            try (final Stream<String> lines = Files.lines(labelPath)) {
               list.addAll(lines.collect(Collectors.toList()));
               list.remove("");
            } catch (Exception e) {
            }

            // Retrieve messages
            try {
               for (int count = 1; count <= messages.length; count++) {
                  final Message message = messages[count - 1];

                  final String messageFile = saveMessage(message, outputFolder, total, count);
                  LOGGER.debug(folderName + ": Message backup file (" + count + "/" + total + "): " + messageFile);

                  list.add(messageFile);
                  syncData.put(folderName, Math.max(syncData.get(folderName), message.getReceivedDate().getTime()));
                  if (count % FETCH_SIZE == 0) {
                     LOGGER.info(folderName + ": Messages downloaded (" + count + "/" + total + ")");
                  }
               }
            } catch (IndexOutOfBoundsException e) {
               LOGGER.warn("Cannot retrieve message", e);
            }

            // Save label file
            try (FileWriter writer = new FileWriter(labelPath.toFile())) {
               LOGGER.info(folderName + ": Messages done (" + list.size() + "/" + total + ")");
               writer.write(String.join(System.lineSeparator(), list));
            } catch (Exception e) {
            }
         }
      } catch (Exception e) {
         LOGGER.error("Error processing label '" + folder.getFullName(), e);
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

   public static void run(final String propsFile, final String outputFolder, final boolean force, final Date fromDate, final Date toDate) throws GeneralSecurityException, IOException, MessagingException {
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

         // Read sync data
         final Map<String, Long> syncData = openSyncData(outputFolder);

         // Open root folder and all sub-folders
         openFolder(store.getDefaultFolder(), outputFolder, force, fromDate, toDate, syncData);

         // Save sync data
         saveSyncData(outputFolder, syncData);
      } finally {
         store.close();
         LOGGER.info("Disconnected!");
      }
   }

}
