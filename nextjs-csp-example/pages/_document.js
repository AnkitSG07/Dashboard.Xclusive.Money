import Document, { Html, Head, Main, NextScript } from 'next/document';
import crypto from 'crypto';

class MyDocument extends Document {
  static async getInitialProps(ctx) {
    const initialProps = await Document.getInitialProps(ctx);
    const nonce = crypto.randomBytes(16).toString('base64');
    const themeScript = "(function(){console.log('theme script');})();";
    const hash = crypto
      .createHash('sha256')
      .update(themeScript)
      .digest('base64');
    return { ...initialProps, nonce, themeScript, hash };
  }

  render() {
    const { nonce, themeScript, hash } = this.props;
    const csp = [
      "default-src 'self'",
      `script-src 'self' 'nonce-${nonce}' 'sha256-${hash}'`,
      "style-src 'self' 'unsafe-inline'",
      "img-src 'self' data:",
      "font-src 'self' https://fonts.gstatic.com"
    ].join('; ');

    return (
      <Html>
        <Head>
          <meta httpEquiv="Content-Security-Policy" content={csp} />
          <script
            nonce={nonce}
            dangerouslySetInnerHTML={{ __html: themeScript }}
          />
        </Head>
        <body>
          <Main />
          <NextScript nonce={nonce} />
        </body>
      </Html>
    );
  }
}

export default MyDocument;
