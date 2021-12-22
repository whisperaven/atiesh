/*
 * Copyright (C) Hao Feng
 */

package atiesh.utils

// java
import java.io.{ File => JFile, FileInputStream }
import java.security.KeyStore
import java.security.cert.{ X509Certificate, CertificateFactory }
import javax.net.ssl.{ TrustManagerFactory, SSLContext }

/**
 * Not goot at Java PKI, any advice are welcome
 *
 * See:
 *  https://docs.oracle.com/javase/7/docs/technotes/guides/security/certpath/CertPathProgGuide.html
 *  https://stackoverflow.com/questions/18513792/using-sslcontext-with-just-a-ca-certificate-and-no-keystore/18514628#18514628
 */
object PKI {
  val DEFAULT_SSL_PROTOCOL = "TLS"

  def openX509Certificate(path: JFile): X509Certificate =
    try {
      CertificateFactory.getInstance("X.509")
                        .generateCertificate(new FileInputStream(path))
                        .asInstanceOf[X509Certificate]
    } catch {
      case exc: Throwable =>
        throw new PKIException(
          s"cannot open x509 certificate file <${path.getAbsolutePath}>", exc)
    }

  def openX509Certificate(path: String): X509Certificate =
    openX509Certificate(new JFile(path))

  def createSSLContext(caCertificate: X509Certificate,
                       sslProtocol: String): SSLContext =
    try {
      val tmf = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm())
      val ks = KeyStore.getInstance(KeyStore.getDefaultType())

      ks.load(null)
      ks.setCertificateEntry(caCertificate.getIssuerX500Principal().getName(),
                             caCertificate)
      tmf.init(ks)

      val ctx = SSLContext.getInstance(sslProtocol)
      ctx.init(null, tmf.getTrustManagers(), null)
      ctx
    } catch {
      case exc: Throwable =>
        throw new PKIException(
          s"cannot create PKI SSLContext using x509 " +
          s"certificate <${caCertificate.getIssuerX500Principal().toString}>" +
          s"with given ssl protocol <${sslProtocol}>", exc)
    }

  def createSSLContext(caCertificate: X509Certificate): SSLContext =
    createSSLContext(caCertificate, DEFAULT_SSL_PROTOCOL)

  def createSSLContext(sslProtocol: String): SSLContext =
    try {
      SSLContext.getInstance(sslProtocol)
    } catch {
      case exc: Throwable =>
        throw new PKIException(
          s"cannot create PKI SSLContext with given " +
          s"ssl protocol <${sslProtocol}>", exc)
    }

  def createSSLContext(): SSLContext = createSSLContext(DEFAULT_SSL_PROTOCOL)
}
