package org.apache.iotdb.db.newsync.transport.client;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.transport.conf.TransportConstant;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.sync.sender.transfer.SyncClient;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.service.transport.thrift.IdentityInfo;
import org.apache.iotdb.service.transport.thrift.MetaInfo;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;
import org.apache.iotdb.service.transport.thrift.TransportService;
import org.apache.iotdb.service.transport.thrift.TransportStatus;
import org.apache.iotdb.service.transport.thrift.Type;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.apache.iotdb.db.newsync.transport.conf.TransportConfig.isCheckFileDegistAgain;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.REBASE_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.RETRY_CODE;
import static org.apache.iotdb.db.newsync.transport.conf.TransportConstant.SUCCESS_CODE;

public class TransportClient implements ITransportClient, Runnable {

  private static final Logger logger = LoggerFactory.getLogger(SyncClient.class);

  // TODO: Need to change to transport config
  private static SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();

  private static final IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();

  private static final int TIMEOUT_MS = 2000_000;

  private TTransport transport = null;

  private TransportService.Client serviceClient = null;

  private String ipAddress = null;

  private int port = -1;

  private String uuid = null;

  private IdentityInfo identityInfo = null;

  private Pipe pipe = null;

  @TestOnly
  private TransportClient() {
    Thread.currentThread().setName(ThreadName.SYNC_CLIENT.getName());
  }

  @TestOnly
  public static TransportClient getInstance() {
    return TransportClient.InstanceHolder.INSTANCE;
  }

  @TestOnly
  public void setServerConfig(String ipAddress, int port) throws IOException {
    this.ipAddress = ipAddress;
    this.port = port;
    this.uuid = getOrCreateUUID(getUuidFile());
  }

  public TransportClient(Pipe pipe, String ipAddress, int port) throws IOException {
    Thread.currentThread().setName(ThreadName.SYNC_CLIENT.getName());
    RpcTransportFactory.setThriftMaxFrameSize(ioTDBConfig.getThriftMaxFrameSize());

    this.pipe = pipe;
    this.ipAddress = ipAddress;
    this.port = port;
    this.uuid = getOrCreateUUID(getUuidFile());

    handshake();
  }

  private boolean handshake() {
    int handshakeCounter = 0;
    while (!handshakeWithVersion()) {
      handshakeCounter++;
      if (handshakeCounter > config.getMaxNumOfSyncFileRetry()) {
        logger.error(
            String.format(
                "Handshake failed %s times! Check network.", config.getMaxNumOfSyncFileRetry()));
        return false;
      }
    }
    return true;
  }

  private boolean handshakeWithVersion() {

    if (transport != null && transport.isOpen()) {
      transport.close();
    }

    try (Socket socket = new Socket(this.ipAddress, this.port)) {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              config.getServerIp(), config.getServerPort(), TIMEOUT_MS);
      TProtocol protocol;
      if (ioTDBConfig.isRpcThriftCompressionEnable()) {
        protocol = new TCompactProtocol(transport);
      } else {
        protocol = new TBinaryProtocol(transport);
      }
      serviceClient = new TransportService.Client(protocol);

      // Underlay socket open.
      if (!transport.isOpen()) {
        transport.open();
      }

      identityInfo =
          new IdentityInfo(
              socket.getLocalAddress().getHostAddress(),
              this.uuid,
              ioTDBConfig.getIoTDBMajorVersion());
      TransportStatus status = serviceClient.handshake(identityInfo);
      if (status.code != SUCCESS_CODE) {
        throw new SyncConnectionException(
            "The receiver rejected the synchronization task because " + status.msg);
      }
    } catch (TTransportException e) {
      logger.error("Cannot connect to the receiver. ", e);
      // TODO: Do actions with exception.
      return false;
    } catch (SyncConnectionException | TException | IOException e) {
      logger.error("Cannot confirm identity with the receiver. ", e);
      // TODO: Do actions with exception.
      return false;
    }
    return true;
  }

  private boolean senderTransport(PipeData pipeData) {

    int retryCount = 0;

    while (true) {
      retryCount++;
      if (retryCount > config.getMaxNumOfSyncFileRetry()) {
        logger.error(
            String.format("After %s tries, stop the transport of current pipeData!", retryCount));
        return false;
      }

      try {
        if (pipeData instanceof TsFilePipeData) {
          for (File file : ((TsFilePipeData) pipeData).getTsFiles()) {
            transportSingleFile(file);
          }
        }
        transportPipeData(pipeData);
        logger.info("Finish current pipeData transport!");
        break;
      } catch (SyncConnectionException e) {
        // handshake and retry
        handshake();
      } catch (IOException | NoSuchAlgorithmException e) {
        logger.error("Transport failed. ", e);
        return false;
      }
    }
    return true;
  }

  /** Transfer data of a tsfile to the receiver. */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void transportSingleFile(File file)
      throws SyncConnectionException, IOException, NoSuchAlgorithmException {

    MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

    while (true) {
      transportSingleFilePieceByPiece(file, messageDigest);

      if (isCheckFileDegistAgain) {
        // Check file digest as entirety.
        messageDigest.reset();
        try (InputStream inputStream = new FileInputStream(file)) {
          byte[] block = new byte[TransportConstant.DATA_CHUNK_SIZE];
          int length;
          while ((length = inputStream.read(block)) > 0) {
            messageDigest.update(block, 0, length);
          }
        }
        MetaInfo metaInfo = new MetaInfo(Type.FILE, file.getName(), 0);

        TransportStatus status = null;

        int retryCount = 0;

        while (true) {
          retryCount++;
          if (retryCount > config.getMaxNumOfSyncFileRetry()) {
            throw new SyncConnectionException(
                String.format(
                    "Can not sync file %s after %s tries.",
                    file.getAbsoluteFile(), config.getMaxNumOfSyncFileRetry()));
          }
          try {
            status =
                serviceClient.checkFileDigest(
                    identityInfo, metaInfo, ByteBuffer.wrap(messageDigest.digest()));
          } catch (TException e) {
            // retry
            logger.error("TException happens! ", e);
            continue;
          }
          break;
        }

        if (status.code != SUCCESS_CODE) {
          logger.error("Digest check of tsfile {} failed, retry", file.getAbsoluteFile());
          continue;
        }
      }
      break;
    }

    logger.info("Receiver has received {} successfully.", file.getAbsoluteFile());
  }

  private void transportSingleFilePieceByPiece(File file, MessageDigest messageDigest)
      throws SyncConnectionException {

    // Cut the file into pieces to send
    long position = 0;

    // Try small piece to rebase the file position.
    byte[] buffer = new byte[1024 * 1024];

    outer:
    while (true) {

      // Normal piece.
      if (position != 0L && buffer.length != TransportConstant.DATA_CHUNK_SIZE) {
        buffer = new byte[TransportConstant.DATA_CHUNK_SIZE];
      }

      int dataLength;
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
          ByteArrayOutputStream byteArrayOutputStream =
              new ByteArrayOutputStream(TransportConstant.DATA_CHUNK_SIZE)) {

        randomAccessFile.seek(position);
        while ((dataLength = randomAccessFile.read(buffer)) != -1) {
          messageDigest.reset();
          byteArrayOutputStream.write(buffer, 0, dataLength);
          messageDigest.update(buffer, 0, dataLength);
          ByteBuffer buffToSend = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
          byteArrayOutputStream.reset();
          MetaInfo metaInfo = new MetaInfo(Type.FILE, file.getName(), position);

          TransportStatus status = null;
          int retryCount = 0;
          while (true) {
            retryCount++;
            if (retryCount > config.getMaxNumOfSyncFileRetry()) {
              throw new SyncConnectionException(
                  String.format(
                      "Can not sync file %s after %s tries.",
                      file.getAbsoluteFile(), config.getMaxNumOfSyncFileRetry()));
            }
            try {
              status =
                  serviceClient.transportData(
                      identityInfo, metaInfo, buffToSend, ByteBuffer.wrap(messageDigest.digest()));
            } catch (TException e) {
              // retry
              logger.error("TException happened! ", e);
              continue;
            }
            break;
          }

          if (status.code == REBASE_CODE) {
            position = Long.parseLong(status.msg);
            continue outer;
          } else if (status.code == RETRY_CODE) {
            logger.info(
                "Receiver failed to receive data from {} because {}, retry.",
                file.getAbsoluteFile(),
                status.msg);
            continue outer;
          } else if (status.code != SUCCESS_CODE) {
            logger.info(
                "Receiver failed to receive data from {} because {}, abort.",
                file.getAbsoluteFile(),
                status.msg);
            throw new RuntimeException("Error! Replace this exception!");
          } else { // Success
            position += dataLength;
          }
        }
      } catch (IOException e) {
        // retry
        logger.error("IOException happened! ", e);
      } catch (SyncConnectionException e) {
        logger.error("Cannot sync data with receiver. ", e);
        throw e;
      }
    }
  }

  private void transportPipeData(PipeData pipeData)
      throws SyncConnectionException, NoSuchAlgorithmException {

    MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

    int retryCount = 0;
    while (true) {

      retryCount++;
      if (retryCount > config.getMaxNumOfSyncFileRetry()) {
        throw new SyncConnectionException(
            String.format(
                "Can not sync pipe data after %s tries.", config.getMaxNumOfSyncFileRetry()));
      }

      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
        int dataLength = new Long(pipeData.serialize(dataOutputStream)).intValue();
        byte[] buffer = new byte[dataLength];

        byteArrayOutputStream.write(buffer, 0, dataLength);
        messageDigest.reset();
        messageDigest.update(buffer, 0, dataLength);
        ByteBuffer buffToSend = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
        byteArrayOutputStream.reset();

        MetaInfo metaInfo =
            new MetaInfo(Type.findByValue(pipeData.getType().ordinal()), "fileName", 0);
        TransportStatus status =
            serviceClient.transportData(
                identityInfo, metaInfo, buffToSend, ByteBuffer.wrap(messageDigest.digest()));

        if (status.code == SUCCESS_CODE) {
          break;
        } else {
          logger.error("Digest check of pipeData failed, retry");
        }
      } catch (IOException | TException e) {
        // retry
        logger.error("Exception happened!", e);
      }
    }
  }

  /** UUID marks the identity of sender for receiver. */
  private String getOrCreateUUID(File uuidFile) throws IOException {
    if (!uuidFile.getParentFile().exists()) {
      uuidFile.getParentFile().mkdirs();
    }

    String uuid;
    if (uuidFile.exists()) {
      try (BufferedReader bf = new BufferedReader((new FileReader(uuidFile)))) {
        uuid = bf.readLine();
      } catch (IOException e) {
        logger.error("Cannot read UUID from file {}", uuidFile.getPath());
        throw new IOException(e);
      }

      if ((uuid == null) || (uuid.length() == 0)) {
        logger.warn("UUID in file {} is empty.", uuidFile.getPath());
        uuidFile.delete();
      } else {
        return uuid;
      }
    }

    // uuidFile not exist or uuid in uuidFile is invalid
    try (FileOutputStream out = new FileOutputStream(uuidFile)) {
      uuid = generateUUID();
      out.write(uuid.getBytes());
    } catch (IOException e) {
      logger.error("Cannot insert UUID to file {}", uuidFile.getPath());
      throw new IOException(e);
    }

    return uuid;
  }

  private String generateUUID() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  private File getUuidFile() {
    return new File(ioTDBConfig.getSyncDir(), SyncConstant.UUID_FILE_NAME);
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's <code>run</code> method to be called in that separately
   * executing thread.
   *
   * <p>The general contract of the method <code>run</code> is that it may take any action
   * whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {

    //    while (true) {
    //      List<PipeData> pipeDataList = this.pipe.pull(Long.MAX_VALUE);
    //      for (PipeData pipeData: pipeDataList) {
    //        boolean sendResult = senderTransport(pipeData);
    //        if (!sendResult) {
    //          // Cannot handle the error.
    //          return;
    //        }
    //        this.pipe.commit(pipeData.getSerialNumber());
    //      }
    //
    //      try {
    //        Thread.sleep(1000);
    //      } catch (InterruptedException e) {
    //        Thread.currentThread().interrupt();
    //        throw new RuntimeException(e);
    //      }
    //    }

  }

  public SyncResponse heartbeat(SyncRequest syncRequest) throws TException {
    return serviceClient.heartbeat(identityInfo, syncRequest);
  }

  @TestOnly
  public static void main(String[] args) throws IOException {

    TransportClient.getInstance().setServerConfig("127.0.0.1", 5555);

    if (!TransportClient.getInstance().handshake()) {
      // Deal with the error here.
      return;
    }

    // Example 1. Send TSFILE.
    //    List<File> files = new ArrayList<>();
    //    files.add(new File(System.getProperty(IoTDBConstant.IOTDB_HOME) + "/files/test1"));
    //    files.add(new File(System.getProperty(IoTDBConstant.IOTDB_HOME) + "/files/test2"));
    //    files.add(new File(System.getProperty(IoTDBConstant.IOTDB_HOME) + "/files/test3"));

    // if (!TransportClient.getInstance().senderTransport()) {
    // Deal with the error here.
    // }

    // Example 2. Send DELETION
    // TsFilePipeData.Type.DELETION.name();

    // Example 3. Send PHYSICALPLAN
    // TsFilePipeData.Type.PHYSICALPLAN.name();
  }

  private static class InstanceHolder {
    private static final TransportClient INSTANCE = new TransportClient();
  }
}
