export default {
  async fetch(request) {
    const params = new URL(request.url).searchParams;
    let target = params.get('url');
    const name = params.get('name');

    if (!target || !name) {
      return new Response('Missing params', { status: 400 });
    }

    if (target.includes('dropbox.com') && target.includes('dl=0')) {
        target = target.replace('dl=0', 'dl=1');
    }

    const rangeHeader = request.headers.get('Range');

    const response = await fetch(target, {
      redirect: 'follow', 
      headers: {
        'Range': rangeHeader || '',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
      }
    });

    const newHeaders = new Headers(response.headers);

    newHeaders.set('Content-Type', 'application/octet-stream');
    
    newHeaders.set('Content-Disposition', `attachment; filename="${name}"`);

    newHeaders.set('X-Content-Type-Options', 'nosniff');

    newHeaders.set('Access-Control-Allow-Origin', '*');
    newHeaders.set('Access-Control-Expose-Headers', 'Content-Range, Content-Length, Accept-Ranges');

    return new Response(response.body, {
      status: response.status,
      headers: newHeaders
    });
  }
}