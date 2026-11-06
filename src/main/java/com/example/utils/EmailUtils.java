package com.example.utils;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class EmailUtils {
    // 163邮箱SMTP配置
    private static final String SMTP_HOST = "smtp.163.com";              // 163邮箱SMTP服务器
    private static final String SMTP_PORT = "465";                      // SSL端口（163邮箱推荐使用SSL）
    private static final String SMTP_USERNAME = "15156616710@163.com";    // 您的163邮箱
    private static final String SMTP_PASSWORD = "LDjLc36XPgW9VTJu"; // 163邮箱授权码
    private static final String FROM_EMAIL = "15156616710@163.com";        // 发件人邮箱
    private static final String TO_EMAIL = "15156616710@163.com";          // 收件人邮箱

    public static void sendEmail(String subject, String message) throws Exception {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.ssl.enable", "true");        // 163邮箱使用SSL
        props.put("mail.smtp.host", SMTP_HOST);
        props.put("mail.smtp.port", SMTP_PORT);
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");

        Session session = Session.getInstance(props, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(SMTP_USERNAME, SMTP_PASSWORD);
            }
        });

        Message mimeMessage = new MimeMessage(session);
        mimeMessage.setFrom(new InternetAddress(FROM_EMAIL));
        mimeMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(TO_EMAIL));
        mimeMessage.setSubject(subject);
        mimeMessage.setText(message);

        Transport.send(mimeMessage);
        System.out.println("163邮件发送成功!");
    }
}