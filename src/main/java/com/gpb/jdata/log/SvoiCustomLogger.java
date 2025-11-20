package com.gpb.jdata.log;


import com.gpb.jdata.config.DatabaseConfig;
import com.gpb.jdata.logrepository.Log;
import com.gpb.jdata.logrepository.LogRepository;
import com.gpb.jdata.orda.properties.OrdProperties;
import com.gpb.jdata.properties.LogsDatabaseProperties;
import com.gpb.jdata.properties.SysProperties;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class SvoiCustomLogger {
    private final SysProperties sysProperties;
    private final LogsDatabaseProperties logsDatabaseProperties;
    private final LogRepository logRepository;
    private final static String username = System.getProperty("user.name");

    private final String ordHostname;
    private final String ordIp;
    private final int ordPort;
    private final String ordUser;

    private final String localHostName;
    private final String localHostAddress;

    private final String gpHostname;
    private final String gpIp;
    private final int gpPort;
    private final String gpUser;

    @Value("${server.port}")
    private Integer localPort;

    private final SvoiJournalFactory svoiJournalFactory = new SvoiJournalFactory();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");

    @Autowired
    public SvoiCustomLogger(SysProperties sysProperties,
                            LogsDatabaseProperties logsDatabaseProperties,
                            LogRepository logRepository,
                            OrdProperties ordProperties,
                            DatabaseConfig databaseConfig) {
        this.sysProperties = sysProperties;
        this.logsDatabaseProperties = logsDatabaseProperties;
        this.logRepository = logRepository;

        try {
            String baseUrl = ordProperties.getBaseUrl();
            if (baseUrl == null || baseUrl.trim().isEmpty()) {
                throw new IllegalArgumentException("Base URL cannot be null or empty");
            }
            
            URL parsedUrl = new URL(baseUrl);
            String host = parsedUrl.getHost();
            
            if (host == null || host.trim().isEmpty()) {
                throw new IllegalArgumentException("Invalid URL: host is missing");
            }
            
            int port = parsedUrl.getPort();
            
            // Определяем порт по умолчанию на основе протокола
            if (port == -1) {
                String protocol = parsedUrl.getProtocol();
                port = "https".equalsIgnoreCase(protocol) ? 443 : 80;
            }
            
            // Резолвим hostname в IP
            InetAddress inetAddress = InetAddress.getByName(host);
            
            this.ordPort = port;
            this.ordIp = inetAddress.getHostAddress();
            this.ordHostname = host;
            this.ordUser = ordProperties.getUsername();
            
            log.info("ORD service configured - Host: {}, IP: {}, Port: {}", 
                    ordHostname, ordIp, ordPort);

            String localHostName;
            String localHostAddress;
            try {
                localHostName = InetAddress.getLocalHost().getHostName();
                localHostAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                localHostName = InetAddress.getLoopbackAddress().getHostName();
                localHostAddress = InetAddress.getLoopbackAddress().getHostAddress();
            }
            this.localHostName = localHostName;
            this.localHostAddress = localHostAddress;

            String gpUrl = databaseConfig.getUrl();

            String hostPort = gpUrl.replace("jdbc:postgresql://", "").split("/")[0];
            String[] parts = hostPort.split(":");
            String src = parts[0];
            String dst;
            String dhost;
            if (isIp(src)) {
                dst = src;
                dhost = resolveHost(src);
            } else {
                dst = resolveIp(src);
                dhost = src;
            }
            this.gpHostname = dhost;
            this.gpIp = dst;
            this.gpPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 5432;
            this.gpUser = databaseConfig.getUsername();
            
        } catch (MalformedURLException e) {
            String errorMsg = String.format("Invalid URL format in OrdProperties: %s", 
                                        ordProperties.getBaseUrl());
            log.error(errorMsg, e);
            throw new IllegalArgumentException(errorMsg, e);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Unexpected error during ORD log configuration", e);
            throw new RuntimeException("Failed to initialize ORD configuration", e);
        }
    }

    public void logConnectToSource() {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setSrc(localHostAddress);
            journal.setShost(localHostName);
            journal.setSpt(localPort);
            journal.setSuser(username);
            journal.setDhost(gpHostname);
            journal.setDst(gpIp);
            journal.setDvchost(gpHostname);
            journal.setDpt(gpPort);
            journal.setDuser(gpUser);

            String message = String.format("connectToGreenPlum dns=%s ip=%s port=%d",
                    gpHostname, gpIp, gpPort);

            send("connectToSource", "Database Connection", message,
                    SvoiSeverityEnum.ONE, journal);

        } catch (Exception e) {
            log.error("Ошибка при логировании подключения к источнику Greenplum", e);
        }
    }

    /** Ошибка подключения или авторизации к базе источника */
    public void logDbConnectionError(Exception e) {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setSrc(localHostAddress);
            journal.setShost(localHostName);
            journal.setSpt(localPort);
            journal.setSuser(username);
            journal.setDhost(gpHostname);
            journal.setDst(gpIp);
            journal.setDvchost(gpHostname);
            journal.setDpt(gpPort);
            journal.setDuser(gpUser);

            String message = String.format(
                    "dbConnectionError connectToGreenPlum user=%s dns=%s ip=%s port=%d error=%s",
                    username,
                    gpHostname,
                    gpIp,
                    gpPort,
                    (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
            );

            send("dbConnectionError",
                    "DB Connection Error",
                    message,
                    SvoiSeverityEnum.FIVE,
                    journal);

        } catch (Exception ex) {
            log.error("Ошибка при логировании dbConnectionError", ex);
        }
    }

    public void logAuth(String ip, String username) {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setSrc(ip);
            journal.setShost(ip);
            journal.setSuser(username);

            String message = String.format(
                    "Authenticated user=%s ip=%s",
                    username != null ? username : "unknown",
                    ip
            );

            send("authSuccess", "Authentication success", message,
                    SvoiSeverityEnum.FIVE, journal);

        } catch (Exception ex) {
            log.error("Ошибка при логировании неверных учётных данных", ex);
        }
    }

    public void logBadCredentials(String ip, String username, String endpoint) {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setSrc(ip);
            journal.setShost(ip);
            journal.setSuser(username);

            String message = String.format(
                    "authFailed invalidCredentials user=%s endpoint=%s ip=%s",
                    username != null ? username : "unknown",
                    endpoint,
                    ip
            );

            send("authFailed", "Invalid Login or Password", message,
                    SvoiSeverityEnum.FIVE, journal);

        } catch (Exception ex) {
            log.error("Ошибка при логировании неверных учётных данных", ex);
        }
    }

    public void logApiCall(HttpServletRequest request, String message) {
        try {
            String clientIp = request.getRemoteAddr();
            String clientHost = request.getRemoteHost();
            int clientPort = request.getRemotePort();

            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setShost(clientHost);
            journal.setSrc(clientIp);
            journal.setSpt(clientPort);

            send("apiCall", "API Request", message, SvoiSeverityEnum.ONE, journal);

        } catch (Exception e) {
            log.error("Ошибка при логировании вызова API", e);
        }
    }

    public void logOrdaCall(String message) {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setShost(localHostName);
            journal.setSrc(localHostAddress);
            journal.setSpt(localPort);
            journal.setDpt(ordPort);
            journal.setDhost(ordHostname);
            journal.setDvchost(ordHostname);
            journal.setDst(ordIp);
            journal.setDuser(ordUser);

            send("apiOrd", "API Request", message, SvoiSeverityEnum.ONE, journal);

        } catch (Exception e) {
            log.error("Ошибка при логировании вызова API", e);
        }
    }

    public void send(String deviceEventClassID, String name, String message, SvoiSeverityEnum severity, SvoiJournal journal) {
        journal.setDeviceProduct(sysProperties.getName());
        journal.setDeviceVersion(sysProperties.getVersion());
        if (journal.getDpt() == null) {
            journal.setDpt(localPort);
        }
        journal.setDntdom(sysProperties.getDntdom());
        journal.setDeviceEventClassID(deviceEventClassID);
        journal.setName(name);
        journal.setMessage(message);
        if (journal.getDhost() == null) {
            journal.setDhost(localHostName);
        }
        if (journal.getDvchost() == null) {
            journal.setDvchost(localHostName);
        }
        if (journal.getDst() == null) {
            journal.setDst(localHostAddress);
        }
        if (journal.getDuser() == null) {
            journal.setDuser(username);
        }
        journal.setSuser(username);
        journal.setApp("https");
        journal.setDmac(getMacAddress());
        journal.setSeverity(severity);

        try (
                MDC.MDCCloseable hostClosable = MDC.putCloseable("host", journal.getHostForSvoi());
                MDC.MDCCloseable logTypeClosable = MDC.putCloseable("log_type", "audit_log");
        ) {
            log.info(StringUtils.replace(journal.toString(), "OmniPlatform", "ORD"));
        }

        if (!logsDatabaseProperties.isEnabled())
            return;

        try {
            LocalDateTime created = LocalDateTime.parse(journal.getStart(), formatter);
            logRepository.save(new Log(created,
                    StringUtils.replace(journal.toString(), "OmniPlatform", "ORD"),
                    deviceEventClassID));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void send(String deviceEventClassID, String name, String message, SvoiSeverityEnum severity) {
        String localHostName = "";
        String localHostAddress = "";
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
            localHostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            localHostName = InetAddress.getLoopbackAddress().getHostName();
            localHostAddress = InetAddress.getLoopbackAddress().getHostAddress();
        }
        SvoiJournal svoiJournal = svoiJournalFactory.getJournalSource();
        svoiJournal.setDeviceProduct(sysProperties.getName());
        svoiJournal.setDeviceVersion(sysProperties.getVersion());
        svoiJournal.setSpt(localPort);
        svoiJournal.setSrc(localHostAddress);
        svoiJournal.setShost(localHostName);
        svoiJournal.setDpt(localPort);
        svoiJournal.setDntdom(sysProperties.getDntdom());
        svoiJournal.setDeviceEventClassID(deviceEventClassID);
        svoiJournal.setName(name);
        svoiJournal.setMessage(message);
        svoiJournal.setDhost(localHostName);
        svoiJournal.setDvchost(localHostName);
        svoiJournal.setDst(localHostAddress);
        svoiJournal.setDuser(username);
        svoiJournal.setSuser(username);
        svoiJournal.setApp("https");
        svoiJournal.setDmac(getMacAddress());
        svoiJournal.setSeverity(severity);
        try (
                MDC.MDCCloseable hostClosable = MDC.putCloseable("host", svoiJournal.getHostForSvoi());
                MDC.MDCCloseable logTypeClosable = MDC.putCloseable("log_type", "audit_log");
        ) {
            log.info(StringUtils.replace(svoiJournal.toString(), "OmniPlatform", "ORD"));
        }
        if (!logsDatabaseProperties.isEnabled())
            return;
        try {
            LocalDateTime created = LocalDateTime.parse(svoiJournal.getStart(), formatter);
            logRepository.save(new Log(created, StringUtils.replace(svoiJournal.toString(), "OmniPlatform", "ORD"), deviceEventClassID));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    private String getMacAddress() {
        List<String> addresses = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            NetworkInterface networkInterface;
            while (networkInterfaceEnumeration.hasMoreElements()) {
                networkInterface = networkInterfaceEnumeration.nextElement();
                byte[] mac = networkInterface.getHardwareAddress();
                if (mac == null)
                    return String.join(":", addresses);
                for (byte b : mac) {
                    addresses.add(String.format("%02X", b));
                }
            }
        } catch (SocketException e) {
            log.error(e.getMessage(), e);
        }
        return String.join(":", addresses);
    }

    private boolean isIp(String input) {
        if (input == null) return false;
        return input.contains(".") || input.contains(":");
    }

    private String resolveHost(String input) {
        try {
            return InetAddress.getByName(input).getHostName();
        } catch (Exception e) {
            return "UnableToResolve:" + input;
        }
    }

    private String resolveIp(String input) {
        try {
            return InetAddress.getByName(input).getHostAddress();
        } catch (Exception e) {
            return "UnableToResolve:" + input;
        }
    }
}
