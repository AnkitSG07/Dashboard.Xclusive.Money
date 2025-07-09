import Head from 'next/head';
import crypto from 'crypto';

export async function getServerSideProps() {
  const inlineScript = "console.log('Inline script works')";
  const hash = crypto.createHash('sha256').update(inlineScript).digest('base64');
  return { props: { inlineScript, hash } };
}

export default function Home({ inlineScript, hash }) {
  const csp = `default-src 'self'; script-src 'self' 'sha256-${hash}'`; 
  return (
    <>
      <Head>
        <meta httpEquiv="Content-Security-Policy" content={csp} />
        <title>CSP Example</title>
        <script dangerouslySetInnerHTML={{ __html: inlineScript }} />
      </Head>
      <main>
        <h1>Next.js CSP Example</h1>
      </main>
    </>
  );
}
