const BAD_INSTANCE_CACHE = {};
const BAD_INSTANCE_DEFAULT_MS = 5 * 60 * 1000;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: corsHeaders()
      });
    }

    if (path === '/' || path === '/index.html') {
      return new Response(HTML_PAGE, {
        headers: {
          'Content-Type': 'text/html; charset=utf-8',
          ...corsHeaders()
        }
      });
    }

    // Entrada PS3-safe para o bloco "Test another video".
    // Evita depender de JavaScript no navegador do PS3.
    // /go?v=VIDEO_ID&type=ps3
    // /go?v=VIDEO_ID&type=pc
    // /go?v=VIDEO_ID&type=health
    // /go?v=VIDEO_ID&type=debug
    if (path === '/go') {
      const rawVideoId = url.searchParams.get('v') || '';
      const videoId = cleanVideoIdInput(rawVideoId);
      const type = String(url.searchParams.get('type') || 'ps3').toLowerCase();

      if (!isValidVideoId(videoId)) {
        return new Response(ERROR_HTML(rawVideoId, 'ID do vídeo inválido. Cole apenas o ID de 11 caracteres ou uma URL do YouTube.'), {
          status: 400,
          headers: {
            'Content-Type': 'text/html; charset=utf-8',
            ...corsHeaders()
          }
        });
      }

      if (type === 'pc') {
        return Response.redirect(`${url.origin}/video/${videoId}.mp4?mode=pc&quality=best`, 302);
      }

      if (type === 'health') {
        return Response.redirect(`${url.origin}/health?v=${videoId}`, 302);
      }

      if (type === 'debug') {
        return Response.redirect(`${url.origin}/debug?v=${videoId}&mode=ps3`, 302);
      }

      return Response.redirect(`${url.origin}/player?v=${videoId}&mode=ps3`, 302);
    }

    // Debug completo.
    // Exemplos:
    // /debug?v=7H6swK9OHC0
    // /debug?v=7H6swK9OHC0&mode=ps3
    // /debug?v=7H6swK9OHC0&mode=pc
    // /debug?v=7H6swK9OHC0&mode=ps3&quality=240p&strict=1
    // /debug?v=7H6swK9OHC0&mode=pc&quality=best
    // /debug?v=7H6swK9OHC0&mode=pc&quality=worst
    if (path === '/debug') {
      const videoId = url.searchParams.get('v');

      if (!isValidVideoId(videoId)) {
        return json({ ok: false, error: 'ID do vídeo inválido' }, 400);
      }

      try {
        const options = getClientOptions(request, url, env);
        const result = await getVideoDebug(videoId, options, env);

        return json(humanDebugResponse(videoId, options, result));

      } catch (error) {
        return json({ ok: false, error: error.message }, 500);
      }
    }



    // Health das APIs.
    // /health?v=7H6swK9OHC0
    // Usa lista dinâmica do api.invidious.io + fallback fixo.
    if (path === '/health') {
      const videoId = url.searchParams.get('v') || '7H6swK9OHC0';

      if (!isValidVideoId(videoId)) {
        return json({ ok: false, error: 'ID do vídeo inválido' }, 400);
      }

      try {
        const apiInfo = await getInvidiousApis(videoId, env, null, 'debug');
        const results = await healthCheck(videoId, env, apiInfo.urls);

        return json({
          ok: true,
          videoId,
          timeoutMs: getApiTimeout(env, null, 'debug'),
          apiSource: apiInfo.source,
          apiCount: apiInfo.urls.length,
          listError: apiInfo.error || '',
          apis: results
        });

      } catch (error) {
        return json({ ok: false, error: error.message }, 500);
      }
    }

    // Player Flash de teste.
    // /player?v=7H6swK9OHC0
    // /player?v=7H6swK9OHC0&mode=ps3
    // /player?v=7H6swK9OHC0&mode=ps3&quality=240p
    if (path === '/player') {
      const videoId = url.searchParams.get('v');

      if (!isValidVideoId(videoId)) {
        return new Response('ID do vídeo inválido', {
          status: 400,
          headers: corsHeaders()
        });
      }

      const options = getClientOptions(request, url, env);

      return new Response(PLAYER_HTML(videoId, url.origin, options), {
        headers: {
          'Content-Type': 'text/html; charset=utf-8',
          ...corsHeaders()
        }
      });
    }

    // ROTA PRINCIPAL PARA O CLASSIC:
    // /video/VIDEO_ID.mp4
    //
    // NÃO precisa mudar sua HTML do classic.
    //
    // Sem query:
    // - PS3 detectado: prioridade 240p -> 360p -> 480p.
    // - PC/celular/outros: best MP4 progressivo.
    //
    // Overrides opcionais:
    // - ?mode=ps3
    // - ?mode=pc
    // - ?quality=240p / 360p / 480p / 720p / best / worst
    // - ?strict=1
    // - ?proxy=1
    if (path.startsWith('/video/') && path.endsWith('.mp4')) {
      const videoId = path.replace('/video/', '').replace('.mp4', '');

      if (!isValidVideoId(videoId)) {
        return new Response('ID do vídeo inválido', {
          status: 400,
          headers: corsHeaders()
        });
      }

      try {
        const options = getClientOptions(request, url, env);
        const result = await getVideoUrl(videoId, options, false, env);

        if (!result.url) {
          return new Response(result.reason || 'Não foi possível extrair MP4 progressivo compatível', {
            status: 503,
            headers: corsHeaders()
          });
        }

        if (url.searchParams.get('proxy') === '1') {
          return await proxyVideo(result.url, request);
        }

        // Direto padrão: igual ao botão "Modo Direto" que funcionou.
        return redirectToVideo(result);

      } catch (error) {
        return new Response(`Erro: ${error.message}`, {
          status: 500,
          headers: corsHeaders()
        });
      }
    }

    // Rota antiga /watch preservada.
    if (path === '/watch') {
      const videoId = url.searchParams.get('v');

      if (!videoId) {
        return new Response('ID do vídeo não encontrado', {
          status: 400,
          headers: corsHeaders()
        });
      }

      if (!isValidVideoId(videoId)) {
        return new Response('ID do vídeo inválido', {
          status: 400,
          headers: corsHeaders()
        });
      }

      try {
        const options = getClientOptions(request, url, env);
        const result = await getVideoUrl(videoId, options, false, env);

        if (!result.url) {
          return new Response(ERROR_HTML(videoId, result.reason), {
            status: 503,
            headers: {
              'Content-Type': 'text/html; charset=utf-8',
              ...corsHeaders()
            }
          });
        }

        if (url.searchParams.get('direct') === '1') {
          return redirectToVideo(result);
        }

        return await proxyVideo(result.url, request);

      } catch (error) {
        return new Response(`Erro: ${error.message}`, {
          status: 500,
          headers: corsHeaders()
        });
      }
    }

    return new Response('Rota não encontrada', {
      status: 404,
      headers: corsHeaders()
    });
  }
};

function isValidVideoId(videoId) {
  return !!videoId && /^[a-zA-Z0-9_-]{11}$/.test(videoId);
}

function cleanVideoIdInput(value) {
  let text = String(value || '').replace(/^\s+|\s+$/g, '');

  if (isValidVideoId(text)) {
    return text;
  }

  const markers = ['v=', 'youtu.be/', '/embed/', '/shorts/', '/live/'];

  for (const marker of markers) {
    const pos = text.indexOf(marker);

    if (pos >= 0) {
      let id = text.substring(pos + marker.length);
      id = id.split('&')[0];
      id = id.split('?')[0];
      id = id.split('#')[0];
      id = id.split('/')[0];

      if (isValidVideoId(id)) {
        return id;
      }
    }
  }

  const match = text.match(/[a-zA-Z0-9_-]{11}/);

  return match && isValidVideoId(match[0]) ? match[0] : '';
}

function isPS3Request(request, url) {
  const ua = request.headers.get('User-Agent') || '';

  return (
    /PS3|PlayStation 3|NetFront/i.test(ua) ||
    url.searchParams.get('mode') === 'ps3'
  );
}

function getClientOptions(request, url, env) {
  const forcedMode = String(url.searchParams.get('mode') || '').toLowerCase();
  const detectedPS3 = isPS3Request(request, url);

  const mode = forcedMode === 'pc'
    ? 'pc'
    : (detectedPS3 ? 'ps3' : 'pc');

  let quality = String(
    url.searchParams.get('quality') ||
    url.searchParams.get('q') ||
    url.searchParams.get('res') ||
    ''
  ).toLowerCase().trim();

  quality = normalizeQuality(quality);

  // Sem mexer no classic:
  // - PS3 usa auto-ps3 por padrão.
  // - PC usa best por padrão.
  //
  // Se quiser trocar padrão sem editar HTML:
  // DEFAULT_PS3_QUALITY=360p / 240p / worst / auto-ps3
  // DEFAULT_PC_QUALITY=best / 720p / worst
  if (!quality) {
    if (mode === 'ps3') {
      quality = normalizeQuality(env.DEFAULT_PS3_QUALITY || '') || 'auto-ps3';
    } else {
      quality = normalizeQuality(env.DEFAULT_PC_QUALITY || '') || 'best';
    }
  }

  const strict = (
    url.searchParams.get('strict') === '1' ||
    String(env.STRICT_QUALITY || '') === '1'
  );

  const apiLimitOverride = parsePositiveInt(url.searchParams.get('apis'), 0);
  const timeoutOverride = parsePositiveInt(url.searchParams.get('timeout'), 0);

  return {
    mode,
    quality,
    detectedPS3,
    strict,
    apiLimitOverride,
    timeoutOverride
  };
}

function normalizeQuality(value) {
  const q = String(value || '').toLowerCase().trim();

  if (!q) return '';

  if (q === 'best' || q === 'auto' || q === 'max') return 'best';
  if (q === 'worst' || q === 'low' || q === 'lowest') return 'worst';
  if (q === 'ps3' || q === 'safe' || q === 'auto-ps3') return 'auto-ps3';

  // itags conhecidos
  if (q === '17') return '144p';
  if (q === '36') return '240p';
  if (q === '18') return '360p';
  if (q === '22') return '720p';

  if (q === '144' || q === '144p') return '144p';
  if (q === '240' || q === '240p' || q === '260' || q === '260p') return '240p';
  if (q === '360' || q === '360p') return '360p';
  if (q === '480' || q === '480p') return '480p';
  if (q === '720' || q === '720p') return '720p';
  if (q === '1080' || q === '1080p') return '1080p';

  return '';
}

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
    'Access-Control-Allow-Headers': 'Range, Content-Type, User-Agent, Accept, Accept-Language',
    'Access-Control-Expose-Headers': 'Content-Length, Content-Range, Accept-Ranges, Content-Type, Location'
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      ...corsHeaders()
    }
  });
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }
}


function getApiTimeout(env, options = null, purpose = 'video') {
  if (options && options.timeoutOverride) {
    const override = Number(options.timeoutOverride);
    if (isFinite(override) && override >= 700 && override <= 7000) return override;
  }

  if (purpose !== 'debug' && options && options.mode === 'ps3') {
    const raw = Number(env.PS3_API_TIMEOUT_MS || 1800);

    if (!isFinite(raw) || raw < 700) return 1800;
    if (raw > 3500) return 3500;

    return raw;
  }

  const raw = Number(env.API_TIMEOUT_MS || 3500);

  if (!isFinite(raw) || raw < 1000) return 3500;
  if (raw > 7000) return 7000;

  return raw;
}

function getApiListTimeout(env) {
  const raw = Number(env.API_LIST_TIMEOUT_MS || 3000);

  if (!isFinite(raw) || raw < 1000) return 3000;
  if (raw > 6000) return 6000;

  return raw;
}

function getApiListLimit(env) {
  const raw = Number(env.API_LIST_LIMIT || 14);

  if (!isFinite(raw) || raw < 6) return 14;
  if (raw > 30) return 30;

  return raw;
}

function getApiLimitForClient(options = null, env = {}, purpose = 'video') {
  if (options && options.apiLimitOverride) {
    const override = Number(options.apiLimitOverride);
    if (isFinite(override) && override >= 1 && override <= 30) return override;
  }

  if (purpose !== 'debug' && options && options.mode === 'ps3') {
    const raw = Number(env.PS3_API_LIMIT || 4);

    if (!isFinite(raw) || raw < 1) return 4;
    if (raw > 8) return 8;

    return raw;
  }

  return getApiListLimit(env);
}

function parsePositiveInt(value, fallback) {
  const n = Number(value || 0);
  if (!isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}


function getFallbackInstanceBases() {
  // Fallback manual caso api.invidious.io caia.
  // Ordem PS3-friendly: primeiro os que funcionaram melhor nos testes reais.
  return [
    'https://inv.thepixora.com',
    'https://iv.melmac.space',
    'https://inv.nadeko.net',
    'https://invidious.nerdvpn.de',
    'https://yt.chocolatemoo53.com',
    'https://invidious.tiekoetter.com',
    'https://invidious.f5.si',
    'https://iv.datura.network',
    'https://iv.nboeck.de',
    'https://vid.puffyan.us'
  ];
}

function normalizeInstanceBase(value) {
  let uri = String(value || '').trim();

  if (!uri) return '';
  if (!/^https?:\/\//i.test(uri)) uri = 'https://' + uri;

  try {
    const u = new URL(uri);

    if (u.protocol !== 'https:') return '';

    return u.origin;
  } catch(e) {
    return '';
  }
}

function uniqPush(list, seen, value) {
  const base = normalizeInstanceBase(value);

  if (!base) return;
  if (seen[base]) return;

  seen[base] = true;
  list.push(base);
}

async function getDynamicInstanceBases(env) {
  const listUrl = String(env.INVIDIOUS_INSTANCES_URL || 'https://api.invidious.io/instances.json?sort_by=type,api,users');
  const timeoutMs = getApiListTimeout(env);

  const response = await fetchWithTimeout(listUrl, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
      'Accept': 'application/json',
      'Accept-Language': 'en-US,en;q=0.9'
    }
  }, timeoutMs);

  if (!response.ok) {
    throw new Error('instances.json HTTP ' + response.status);
  }

  let data;
  try {
    data = await response.json();
  } catch(e) {
    throw new Error('instances.json JSON inválido');
  }

  if (!Array.isArray(data)) {
    throw new Error('instances.json formato inesperado');
  }

  const out = [];
  const seen = {};

  for (const item of data) {
    // Formato comum:
    // [ "domain.tld", { uri: "https://domain.tld", type: "https", api: true, ... } ]
    let domain = '';
    let meta = null;

    if (Array.isArray(item)) {
      domain = item[0] || '';
      meta = item[1] || null;
    } else if (item && typeof item === 'object') {
      domain = item.name || item.domain || '';
      meta = item;
    }

    if (!meta || typeof meta !== 'object') continue;

    const type = String(meta.type || '').toLowerCase();
    const uri = meta.uri || domain;

    // A lista pública pode ter instância web sem API.
    // Para nosso Worker, só vale quem tem API e HTTPS.
    if (meta.api === false) continue;
    if (type && type !== 'https') continue;

    uniqPush(out, seen, uri);
  }

  return out;
}

async function getInvidiousApis(videoId, env, options = null, purpose = 'video') {
  const limit = getApiLimitForClient(options, env, purpose);
  const merged = [];
  const seen = {};
  let source = 'fallback';
  let error = '';

  // Favoritas primeiro: evita depender da ordem dinâmica e mantém comportamento conhecido.
  for (const base of getFallbackInstanceBases()) {
    uniqPush(merged, seen, base);
  }

  const useDynamic = String(env.DISABLE_DYNAMIC_INSTANCES || '') !== '1' && !(
    purpose !== 'debug' &&
    options &&
    options.mode === 'ps3' &&
    String(env.PS3_USE_DYNAMIC_INSTANCES || '') !== '1'
  );

  if (useDynamic) {
    try {
      const dynamicBases = await getDynamicInstanceBases(env);

      for (const base of dynamicBases) {
        uniqPush(merged, seen, base);
      }

      source = 'dynamic+fallback';

    } catch(e) {
      error = e && e.message ? e.message : String(e);
      source = 'fallback';
    }
  }

  let usableBases = merged;

  if (purpose !== 'debug' && String(env.DISABLE_BAD_INSTANCE_COOLDOWN || '') !== '1') {
    const filtered = merged.filter(base => !isBadInstanceCooldown(base));

    // Se tudo estiver em cooldown, tenta mesmo assim para não deixar sem vídeo.
    if (filtered.length) {
      usableBases = filtered;
    }
  }

  const urls = usableBases
    .slice(0, limit)
    .map(base => `${base}/api/v1/videos/${videoId}`);

  return {
    urls,
    source,
    error,
    limit,
    skippedByCooldown: merged.length - usableBases.length
  };
}

function getInstanceBaseFromApiUrl(apiUrl) {
  try {
    return new URL(apiUrl).origin;
  } catch(e) {
    return '';
  }
}

function isBadInstanceCooldown(base) {
  const key = normalizeInstanceBase(base);
  if (!key) return false;

  const until = BAD_INSTANCE_CACHE[key] || 0;

  return Date.now() < until;
}

function markBadInstance(apiUrl, env = {}, reason = '') {
  const base = getInstanceBaseFromApiUrl(apiUrl);
  if (!base) return;

  const raw = Number(env.BAD_INSTANCE_COOLDOWN_MS || BAD_INSTANCE_DEFAULT_MS);
  const cooldownMs = isFinite(raw) && raw >= 30000 ? raw : BAD_INSTANCE_DEFAULT_MS;

  BAD_INSTANCE_CACHE[base] = Date.now() + cooldownMs;
}

function shouldMarkBadResponse(status) {
  if (status === null || typeof status === 'undefined') return true;
  return status === 401 ||
    status === 403 ||
    status === 404 ||
    status === 408 ||
    status === 429 ||
    status === 500 ||
    status === 502 ||
    status === 503 ||
    status === 520 ||
    status === 522 ||
    status === 523 ||
    status === 524 ||
    status === 530;
}



async function getVideoDebug(videoId, options, env = {}) {
  const apiInfo = await getInvidiousApis(videoId, env, options, 'debug');
  const apis = apiInfo.urls;
  const timeoutMs = getApiTimeout(env, options, 'debug');

  const servers = await Promise.all(apis.map(apiUrl => debugFetchVideoApi(apiUrl, options, timeoutMs)));
  const firstSelectedServer = servers.find(server => server.ok && server.selectedForThisServer);
  const selected = firstSelectedServer ? firstSelectedServer.selectedForThisServer : null;

  return {
    ok: !!selected,
    selected,
    exactMatch: selected ? !!selected.exactForRequest : false,
    reason: selected ? '' : `Nenhuma resolução compatível encontrada para mode=${options.mode}, quality=${options.quality}${options.strict ? ', strict=1' : ''}.`,
    apiSource: apiInfo.source,
    listError: apiInfo.error || '',
    serverCount: servers.length,
    okServerCount: servers.filter(server => server.ok).length,
    servers
  };
}

async function debugFetchVideoApi(apiUrl, options, timeoutMs) {
  const startedAt = Date.now();
  const server = getApiServerName(apiUrl);

  try {
    const response = await fetchWithTimeout(apiUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9'
      }
    }, timeoutMs);

    if (!response.ok) {
      return compactDebugError(server, response.status, Date.now() - startedAt, 'HTTP ' + response.status);
    }

    let data;
    try {
      data = await response.json();
    } catch (e) {
      return compactDebugError(server, response.status, Date.now() - startedAt, 'JSON inválido');
    }

    const formatStreams = Array.isArray(data.formatStreams) ? data.formatStreams : [];
    const adaptiveFormats = Array.isArray(data.adaptiveFormats) ? data.adaptiveFormats : [];
    attachVideoMeta(formatStreams, data);
    attachVideoMeta(adaptiveFormats, data);
    const picked = pickBestFormat(formatStreams, options);

    return {
      server,
      ok: true,
      status: response.status,
      ms: Date.now() - startedAt,
      title: data.title || '',
      directWithAudio: compactFormatList(formatStreams.filter(format => format && format.url)),
      videoOnly: compactFormatList(adaptiveFormats.filter(format => format && format.url && getHeight(format) > 0)),
      chosen: picked ? compactFormatName(picked) : null,
      selectedForThisServer: picked ? debugSelectedFormat(picked, options, server) : null
    };

  } catch (e) {
    return compactDebugError(
      server,
      null,
      Date.now() - startedAt,
      e && e.name === 'AbortError'
        ? 'Timeout ' + timeoutMs + 'ms'
        : (e && e.message ? e.message : String(e))
    );
  }
}

function compactDebugError(server, status, ms, error) {
  return {
    server,
    ok: false,
    status,
    ms,
    error
  };
}

function humanDebugResponse(videoId, options, result) {
  return {
    ok: yesNo(result.ok),
    videoId,
    client: humanDebugClient(options),
    selected: result.selected ? humanSelectedFormat(result.selected) : null,
    exactMatch: yesNo(result.exactMatch),
    reason: result.reason || '',
    apiSource: result.apiSource || '',
    listError: result.listError || '',
    serverCount: formatCount(result.serverCount || 0, 'server', 'servers'),
    okServerCount: formatCount(result.okServerCount || 0, 'server OK', 'servers OK'),
    servers: (result.servers || []).map(humanDebugServer)
  };
}

function humanDebugClient(options) {
  return {
    mode: options.mode,
    quality: options.quality,
    detectedPS3: yesNo(options.detectedPS3),
    strict: yesNo(options.strict)
  };
}

function humanDebugServer(server) {
  if (!server || !server.ok) {
    return {
      server: server && server.server ? server.server : '',
      ok: 'no',
      status: formatHttpStatus(server ? server.status : null),
      ms: formatMs(server ? server.ms : 0),
      error: server && server.error ? server.error : ''
    };
  }

  return {
    server: server.server,
    ok: 'yes',
    status: formatHttpStatus(server.status),
    ms: formatMs(server.ms),
    title: server.title || '',
    directWithAudio: server.directWithAudio || [],
    videoOnly: server.videoOnly || [],
    chosen: server.chosen || 'none',
    selectedForThisServer: server.selectedForThisServer ? humanSelectedFormat(server.selectedForThisServer) : null
  };
}

function humanSelectedFormat(format) {
  return {
    server: format.server || '',
    resolution: format.resolution || '',
    itag: format.itag || '',
    type: format.type || '',
    bitrate: formatBitrate(format.bitrate),
    memoryRisk: format.bitrateRisk || format.risk || '',
    requestMatch: format.exactForRequest ? 'exact' : 'fallback',
    score: formatScore(format.score)
  };
}

function yesNo(value) {
  return value ? 'yes' : 'no';
}

function formatCount(value, singular, plural) {
  const n = Number(value || 0);
  const safe = isFinite(n) ? n : 0;
  return safe + ' ' + (safe === 1 ? singular : plural);
}

function formatMs(value) {
  const n = Number(value || 0);
  if (!isFinite(n) || n <= 0) return '0ms';
  return Math.round(n) + 'ms';
}

function formatHttpStatus(status) {
  if (status === null || typeof status === 'undefined') return 'no response';
  return 'HTTP ' + status;
}

function formatBitrate(value) {
  const n = Number(value || 0);
  if (!isFinite(n) || n <= 0) return 'unknown';
  return Math.round(n / 1000) + 'kbps';
}

function formatScore(value) {
  const n = Number(value || 0);
  if (!isFinite(n)) return '0 pts';
  return Math.round(n) + ' pts';
}

function debugSelectedFormat(format, options, server) {
  return {
    server,
    resolution: getResolutionLabel(format),
    itag: String(format.itag || ''),
    type: compactCodecLabel(format),
    height: getHeight(format),
    bitrate: getBitrate(format),
    bitrateRisk: getBitrateRiskLabel(format),
    exactForRequest: isExactQuality(format, options.quality),
    score: scoreFormat(format, options)
  };
}

function compactFormatList(formats) {
  const list = [];
  const seen = {};

  for (const format of formats || []) {
    if (!format) continue;

    const height = getHeight(format);
    const itag = String(format.itag || '?');
    const name = compactFormatName(format);
    const key = [height, itag, compactCodecLabel(format)].join('|');

    if (seen[key]) continue;
    seen[key] = true;
    list.push({ height, itag, name });
  }

  list.sort((a, b) => {
    if ((a.height || 0) !== (b.height || 0)) return (a.height || 0) - (b.height || 0);
    return String(a.itag).localeCompare(String(b.itag), undefined, { numeric: true });
  });

  return list.map(item => item.name);
}

function compactFormatName(format) {
  const resolution = getResolutionLabel(format);
  const itag = String(format.itag || '?');
  const codec = compactCodecLabel(format);
  const bitrate = getBitrate(format);

  return `${resolution} / itag ${itag}${codec ? ' / ' + codec : ''}${bitrate ? ' / ' + formatBitrate(bitrate) : ''}`;
}

function compactCodecLabel(format) {
  const type = getFormatType(format);
  const container = String(format.container || '').toLowerCase();
  const encoding = String(format.encoding || '').toLowerCase();

  let out = '';

  if (container) {
    out = container;
  } else if (type.includes('webm')) {
    out = 'webm';
  } else if (type.includes('3gpp') || type.includes('3gp')) {
    out = '3gp';
  } else if (type.includes('mp4')) {
    out = 'mp4';
  }

  let codec = '';

  if (encoding) {
    codec = encoding;
  } else if (type.includes('avc1')) {
    codec = 'h264';
  } else if (type.includes('av01')) {
    codec = 'av1';
  } else if (type.includes('vp9')) {
    codec = 'vp9';
  } else if (type.includes('mp4a')) {
    codec = 'aac';
  } else if (type.includes('opus')) {
    codec = 'opus';
  }

  if (type.includes('mp4a') && codec === 'h264') {
    codec = 'h264+aac';
  }

  if (out && codec) return out + '/' + codec;
  return out || codec;
}

function getApiServerName(apiUrl) {
  const host = safeHost(apiUrl);
  return host || apiUrl;
}

function summarizeFormats(formats) {
  return compactFormatList(formats);
}

function getResolutionLabel(format) {
  const label = String(format.qualityLabel || format.quality || '').trim();
  const height = getHeight(format);

  if (label) return label;
  if (height) return height + 'p';

  const itag = String(format.itag || '');
  if (itag === '17') return '144p';
  if (itag === '36') return '240p';
  if (itag === '18') return '360p';
  if (itag === '22') return '720p';

  return 'unknown';
}

function getFormatType(format) {
  return String(format.type || format.mimeType || '').toLowerCase();
}

function getWidth(format) {
  const raw = Number(format.width || 0);
  return isFinite(raw) ? raw : 0;
}

function getFps(format) {
  const raw = Number(format.fps || 0);
  return isFinite(raw) ? raw : 0;
}

function getBitrate(format) {
  const raw = Number(format.bitrate || format.averageBitrate || 0);
  if (isFinite(raw) && raw > 0) return raw;

  const length = Number(format.contentLength || format.clen || format.size || 0);
  const duration = Number(format.__ps3DurationSeconds || format.durationSeconds || format.duration || 0);

  if (isFinite(length) && length > 0 && isFinite(duration) && duration > 0) {
    return Math.round((length * 8) / duration);
  }

  return 0;
}

function getDurationSecondsFromData(data) {
  if (!data || typeof data !== 'object') return 0;

  const raw = Number(data.lengthSeconds || data.length_seconds || data.duration || 0);

  return isFinite(raw) && raw > 0 ? raw : 0;
}

function attachVideoMeta(formats, data) {
  const duration = getDurationSecondsFromData(data);
  if (!duration || !Array.isArray(formats)) return;

  for (const format of formats) {
    if (format && typeof format === 'object') {
      format.__ps3DurationSeconds = duration;
    }
  }
}

function getBitrateRiskLabel(format) {
  const height = getHeight(format);
  const bitrate = getBitrate(format);

  if (height >= 720) return 'danger';
  if (!bitrate) return 'unknown';
  if (bitrate <= 500000) return 'safe';
  if (bitrate <= 800000) return 'medium';
  if (bitrate <= 1200000) return 'heavy';
  return 'danger';
}


async function healthFetch(apiUrl, timeoutMs) {
  const startedAt = Date.now();

  try {
    const response = await fetchWithTimeout(apiUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9'
      }
    }, timeoutMs);

    const type = response.headers.get('content-type') || '';

    if (!response.ok) {
      return {
        apiUrl,
        ok: false,
        status: response.status,
        type,
        ms: Date.now() - startedAt,
        error: 'HTTP ' + response.status,
        title: '',
        formatStreams: 0,
        adaptiveFormats: 0
      };
    }

    let data;
    try {
      data = await response.json();
    } catch (e) {
      return {
        apiUrl,
        ok: false,
        status: response.status,
        type,
        ms: Date.now() - startedAt,
        error: 'JSON inválido',
        title: '',
        formatStreams: 0,
        adaptiveFormats: 0
      };
    }

    return {
      apiUrl,
      ok: true,
      status: response.status,
      type,
      ms: Date.now() - startedAt,
      error: '',
      title: data.title || '',
      formatStreams: Array.isArray(data.formatStreams) ? data.formatStreams.length : 0,
      adaptiveFormats: Array.isArray(data.adaptiveFormats) ? data.adaptiveFormats.length : 0
    };

  } catch (e) {
    return {
      apiUrl,
      ok: false,
      status: null,
      type: '',
      ms: Date.now() - startedAt,
      error: e && e.name === 'AbortError'
        ? 'Timeout ' + timeoutMs + 'ms'
        : (e && e.message ? e.message : String(e)),
      title: '',
      formatStreams: 0,
      adaptiveFormats: 0
    };
  }
}

async function healthCheck(videoId, env, apiUrls) {
  const timeoutMs = getApiTimeout(env, null, 'debug');
  const urls = apiUrls && apiUrls.length ? apiUrls : (await getInvidiousApis(videoId, env, null, 'debug')).urls;

  return await Promise.all(urls.map(apiUrl => healthFetch(apiUrl, timeoutMs)));
}

function getDynamicRescueLimit(options = null, env = {}) {
  if (options && options.apiLimitOverride) {
    const override = Number(options.apiLimitOverride);
    if (isFinite(override) && override >= 1 && override <= 30) return override;
  }

  const raw = Number(env.PS3_DYNAMIC_RESCUE_LIMIT || 8);

  if (!isFinite(raw) || raw < 1) return 8;
  if (raw > 20) return 20;

  return raw;
}

function getDynamicRescueTimeout(env = {}, options = null) {
  if (options && options.timeoutOverride) {
    const override = Number(options.timeoutOverride);
    if (isFinite(override) && override >= 700 && override <= 7000) return override;
  }

  const raw = Number(env.PS3_DYNAMIC_RESCUE_TIMEOUT_MS || 2200);

  if (!isFinite(raw) || raw < 900) return 2200;
  if (raw > 4000) return 4000;

  return raw;
}

function getAttemptedBaseMap(attempts) {
  const map = {};

  for (const attempt of attempts || []) {
    const base = getInstanceBaseFromApiUrl(attempt && attempt.apiUrl ? attempt.apiUrl : '');
    if (base) map[base] = true;
  }

  return map;
}

async function getDynamicRescueApis(videoId, env, options = null, attempts = []) {
  const limit = getDynamicRescueLimit(options, env);
  const fallbackSeen = {};
  const attemptedSeen = getAttemptedBaseMap(attempts);
  let dynamicBases = [];
  let error = '';

  for (const base of getFallbackInstanceBases()) {
    const normalized = normalizeInstanceBase(base);
    if (normalized) fallbackSeen[normalized] = true;
  }

  try {
    dynamicBases = await getDynamicInstanceBases(env);
  } catch(e) {
    error = e && e.message ? e.message : String(e);
    dynamicBases = [];
  }

  const out = [];
  const seen = {};

  for (const base of dynamicBases) {
    const normalized = normalizeInstanceBase(base);
    if (!normalized) continue;
    if (fallbackSeen[normalized]) continue;
    if (attemptedSeen[normalized]) continue;
    if (seen[normalized]) continue;
    if (String(env.DISABLE_BAD_INSTANCE_COOLDOWN || '') !== '1' && isBadInstanceCooldown(normalized)) continue;

    seen[normalized] = true;
    out.push(`${normalized}/api/v1/videos/${videoId}`);
    if (out.length >= limit) break;
  }

  return {
    urls: out,
    source: 'dynamic-rescue',
    error,
    limit,
    skippedByCooldown: dynamicBases.length - out.length
  };
}

function shouldUseDynamicRescue(options, env, primaryResult, attempts) {
  if (!options || options.mode !== 'ps3') return false;
  if (String(env.DISABLE_DYNAMIC_INSTANCES || '') === '1') return false;
  if (String(env.PS3_DYNAMIC_RESCUE || '1') === '0') return false;

  // Usa a lista dinâmica como segunda onda apenas quando os fixos falharam
  // ou quando só apareceu um formato danger. Medium/heavy ainda toca sem atrasar.
  if (!primaryResult) return true;

  const picked = primaryResult.pickedFormat || null;
  if (!picked) return false;

  return shouldKeepSearchingForSaferFormat(picked, options);
}

function mergeVideoApiInfo(primaryInfo, rescueInfo) {
  if (!rescueInfo) return primaryInfo;

  return {
    source: primaryInfo && primaryInfo.source
      ? primaryInfo.source + '+' + rescueInfo.source
      : rescueInfo.source,
    error: [primaryInfo && primaryInfo.error ? primaryInfo.error : '', rescueInfo.error || '']
      .filter(Boolean)
      .join(' | ')
  };
}

async function tryVideoApis(videoId, options, debug, env, apiInfo, apis, attempts, timeoutMs) {
  let fallbackSelectedResult = null;

  for (const apiUrl of apis) {
    try {
      const response = await fetchWithTimeout(apiUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
          'Accept': 'application/json',
          'Accept-Language': 'en-US,en;q=0.9'
        }
      }, timeoutMs);

      const contentType = response.headers.get('content-type') || '';

      if (!response.ok) {
        attempts.push({
          apiUrl,
          ok: false,
          status: response.status,
          type: contentType
        });

        if (shouldMarkBadResponse(response.status)) {
          markBadInstance(apiUrl, env, 'HTTP ' + response.status);
        }

        continue;
      }

      let data;
      try {
        data = await response.json();
      } catch (e) {
        attempts.push({
          apiUrl,
          ok: false,
          status: response.status,
          type: contentType,
          error: 'JSON inválido'
        });
        markBadInstance(apiUrl, env, 'JSON inválido');
        continue;
      }

      const streams = Array.isArray(data.formatStreams) ? data.formatStreams : [];
      attachVideoMeta(streams, data);

      attempts.push({
        apiUrl,
        ok: true,
        status: response.status,
        type: contentType,
        title: data.title || '',
        streamCount: streams.length,
        requested: options,
        streams: debug ? streams.map(f => debugFormat(f, options)) : undefined
      });

      if (!streams.length) {
        continue;
      }

      const picked = pickBestFormat(streams, options);

      if (picked && picked.url) {
        const selectedResult = buildVideoUrlResult(videoId, apiInfo, apiUrl, data, picked, options, attempts, debug);

        if (shouldKeepSearchingForSaferFormat(picked, options)) {
          if (!fallbackSelectedResult || scoreFormat(picked, options) > scoreFormat(fallbackSelectedResult.pickedFormat || {}, options)) {
            fallbackSelectedResult = selectedResult;
          }
          continue;
        }

        return {
          result: selectedResult,
          fallback: fallbackSelectedResult
        };
      }

    } catch (e) {
      attempts.push({
        apiUrl,
        ok: false,
        error: e && e.name === 'AbortError'
          ? 'Timeout ' + timeoutMs + 'ms'
          : (e && e.message ? e.message : String(e))
      });
      markBadInstance(apiUrl, env, e && e.message ? e.message : String(e));
    }
  }

  return {
    result: null,
    fallback: fallbackSelectedResult
  };
}

function chooseBetterFallback(a, b, options) {
  if (!a) return b || null;
  if (!b) return a;

  return scoreFormat(b.pickedFormat || {}, options) > scoreFormat(a.pickedFormat || {}, options) ? b : a;
}

async function getVideoUrl(videoId, options, debug = false, env = {}) {
  const attempts = [];
  const apiInfo = await getInvidiousApis(videoId, env, options, 'video');
  const apis = apiInfo.urls;
  const timeoutMs = getApiTimeout(env, options, 'video');

  const primary = await tryVideoApis(videoId, options, debug, env, apiInfo, apis, attempts, timeoutMs);

  if (primary.result) {
    return primary.result;
  }

  let fallbackSelectedResult = primary.fallback || null;

  if (shouldUseDynamicRescue(options, env, fallbackSelectedResult, attempts)) {
    const rescueInfo = await getDynamicRescueApis(videoId, env, options, attempts);

    if (rescueInfo.urls.length) {
      attempts.push({
        apiUrl: 'dynamic-rescue',
        ok: true,
        status: 0,
        type: 'rescue-list',
        streamCount: rescueInfo.urls.length,
        requested: options
      });

      const mergedInfo = mergeVideoApiInfo(apiInfo, rescueInfo);
      const rescueTimeoutMs = getDynamicRescueTimeout(env, options);
      const rescue = await tryVideoApis(videoId, options, debug, env, mergedInfo, rescueInfo.urls, attempts, rescueTimeoutMs);

      if (rescue.result) {
        return rescue.result;
      }

      fallbackSelectedResult = chooseBetterFallback(fallbackSelectedResult, rescue.fallback, options);
    } else if (rescueInfo.error) {
      attempts.push({
        apiUrl: 'dynamic-rescue',
        ok: false,
        status: null,
        error: rescueInfo.error
      });
    }
  }

  if (fallbackSelectedResult) {
    return fallbackSelectedResult;
  }

  return {
    url: null,
    apiSource: typeof apiInfo !== 'undefined' ? apiInfo.source : '',
    listError: typeof apiInfo !== 'undefined' ? (apiInfo.error || '') : '',
    selected: null,
    exactMatch: false,
    reason: `Nenhum MP4 progressivo compatível encontrado para mode=${options.mode}, quality=${options.quality}${options.strict ? ', strict=1' : ''}.`,
    attempts
  };
}

function buildVideoUrlResult(videoId, apiInfo, apiUrl, data, picked, options, attempts, debug) {
  const bitrate = getBitrate(picked);
  const exactMatch = isExactQuality(picked, options.quality) ||
    options.quality === 'best' ||
    options.quality === 'worst' ||
    options.quality === 'auto-ps3';

  return {
    url: picked.url,
    pickedFormat: picked,
    apiSource: apiInfo.source,
    listError: apiInfo.error || '',
    exactMatch,
    selected: {
      apiUrl,
      title: data.title || '',
      mode: options.mode,
      detectedPS3: options.detectedPS3,
      requestedQuality: options.quality,
      strict: options.strict,
      itag: picked.itag,
      qualityLabel: picked.qualityLabel || '',
      type: picked.type || picked.mimeType || '',
      height: getHeight(picked),
      bitrate,
      bitrateRisk: getBitrateRiskLabel(picked),
      progressiveMp4: isProgressiveMp4(picked),
      exactForRequest: isExactQuality(picked, options.quality),
      host: safeHost(picked.url),
      isGoogleVideo: String(picked.url).includes('googlevideo.com'),
      score: scoreFormat(picked, options),
      preview: debug ? String(picked.url).slice(0, 260) + '...' : undefined
    },
    attempts
  };
}

function shouldKeepSearchingForSaferFormat(format, options) {
  if (!options || options.mode !== 'ps3') return false;
  if (options.quality !== 'auto-ps3' && options.quality !== 'worst') return false;

  const height = getHeight(format);
  const risk = getBitrateRiskLabel(format);

  return height >= 720 || risk === 'danger';
}


function debugFormat(format, options) {
  return {
    itag: format.itag,
    resolution: getResolutionLabel(format),
    qualityLabel: format.qualityLabel || '',
    quality: format.quality || '',
    type: format.type || format.mimeType || '',
    container: format.container || '',
    encoding: format.encoding || '',
    hasUrl: !!format.url,
    host: format.url ? safeHost(format.url) : '',
    height: getHeight(format),
    width: getWidth(format),
    fps: getFps(format),
    bitrate: getBitrate(format),
    bitrateRisk: getBitrateRiskLabel(format),
    contentLength: format.contentLength || format.clen || '',
    progressiveMp4: isProgressiveMp4(format),
    exactForRequest: isExactQuality(format, options.quality),
    exactFor144p: isExactQuality(format, '144p'),
    exactFor240p: isExactQuality(format, '240p'),
    exactFor360p: isExactQuality(format, '360p'),
    exactFor480p: isExactQuality(format, '480p'),
    exactFor720p: isExactQuality(format, '720p'),
    score: scoreFormat(format, options)
  };
}

function pickBestFormat(streams, options) {
  const usable = streams
    .filter(f => f && f.url)
    .filter(f => isProgressiveMp4(f));

  if (!usable.length) {
    return null;
  }

  // Strict: qualidade exata ou erro.
  // Serve só para teste. Não remove fallback padrão.
  if (options.strict && isSpecificQuality(options.quality)) {
    const exact = usable.filter(f => isExactQuality(f, options.quality));
    if (!exact.length) {
      return null;
    }

    exact.sort((a, b) => scoreFormat(b, options) - scoreFormat(a, options));
    return exact[0] || null;
  }

  usable.sort((a, b) => scoreFormat(b, options) - scoreFormat(a, options));
  return usable[0] || null;
}

function isSpecificQuality(q) {
  return q === '144p' ||
    q === '240p' ||
    q === '360p' ||
    q === '480p' ||
    q === '720p' ||
    q === '1080p';
}

function isExactQuality(format, q) {
  const height = getHeight(format);
  const label = String(format.qualityLabel || format.quality || '').toLowerCase();
  const itag = String(format.itag || '');

  if (q === '144p') return height === 144 || label === '144p' || itag === '17';
  if (q === '240p') return height === 240 || label === '240p' || itag === '36';
  if (q === '360p') return height === 360 || label === '360p' || itag === '18';
  if (q === '480p') return height === 480 || label === '480p';
  if (q === '720p') return height === 720 || label === '720p' || itag === '22';
  if (q === '1080p') return height === 1080 || label === '1080p';

  return false;
}

function scoreFormat(format, options) {
  const itag = String(format.itag || '');
  const type = String(format.type || format.mimeType || '').toLowerCase();
  const height = getHeight(format);
  const bitrate = getBitrate(format);

  let score = 0;

  if (format.url) score += 1000;
  if (isProgressiveMp4(format)) score += 1000;
  if (type.includes('video/mp4')) score += 300;

  // itags progressivos conhecidos.
  if (itag === '17') score += 260; // 144p
  if (itag === '36') score += 320; // 240p
  if (itag === '18') score += 300; // 360p
  if (itag === '22') score += 220; // 720p

  // Peso por bitrate: não bloqueia, só escolhe o menos ruim para o PS3.
  if (options.mode === 'ps3') {
    if (bitrate > 0) {
      if (bitrate <= 350000) score += 1600;
      else if (bitrate <= 500000) score += 1100;
      else if (bitrate <= 800000) score += 350;
      else if (bitrate <= 1200000) score -= 700;
      else score -= 1800;
    }

    if (height >= 720 || itag === '22') score -= 7000;
    if (height > 720) score -= 10000;
  }

  // PS3 padrão:
  // prioridade realista: 240p -> 144p -> 360p leve -> 480p.
  // Se só existir 360p pesado, ainda toca; apenas recebe pontuação menor.
  if (options.mode === 'ps3' && options.quality === 'auto-ps3') {
    if (height === 240 || itag === '36') score += 5600;
    if (height === 144 || itag === '17') score += 5200;
    if (height === 360 || itag === '18') score += 4300;
    if (height === 480) score += 3000;
    return score;
  }

  // PC/outros: melhor MP4 progressivo.
  if (options.mode === 'pc' && options.quality === 'best') {
    score += height * 10;
    if (height >= 720) score += 900;
    if (height >= 1080) score += 900;
    if (itag === '22') score += 400;
    return score;
  }

  // Worst preservado:
  // menor MP4 progressivo disponível, com preferência por bitrate menor.
  if (options.quality === 'worst') {
    score -= height * 12;
    if (height && height < 360) score += 3500;
    if (height === 360 || itag === '18') score += 1000;
    if (options.mode === 'ps3' && bitrate > 0) score -= Math.round(bitrate / 1000);
    return score;
  }

  // Qualidade manual:
  // tenta exata primeiro, depois fallback mais próximo.
  if (isSpecificQuality(options.quality)) {
    if (isExactQuality(format, options.quality)) {
      score += 6000;
    } else {
      const target = Number(options.quality.replace('p', ''));
      score -= Math.abs((height || 360) - target) * 7;
    }

    // Se pediu qualidade baixa no PS3, evita subir para 720+.
    if (options.mode === 'ps3' && (options.quality === '144p' || options.quality === '240p')) {
      if (height === 360 || itag === '18') score += 1400;
      if (height === 480) score += 700;
      if (height >= 720) score -= 7000;
    }

    return score;
  }

  // Fallback seguro.
  score += height;
  return score;
}

function getHeight(format) {
  const label = String(format.qualityLabel || format.quality || '').toLowerCase();
  const m = label.match(/(\d{3,4})p/);
  if (m) return Number(m[1]);

  const itag = String(format.itag || '');
  if (itag === '17') return 144;
  if (itag === '36') return 240;
  if (itag === '18') return 360;
  if (itag === '22') return 720;

  return 0;
}

function isProgressiveMp4(format) {
  const type = String(format.type || '').toLowerCase();
  const mime = String(format.mimeType || '').toLowerCase();
  const itag = String(format.itag || '');

  // Para o Flash do PS3, aceitamos também os formatos baixos antigos:
  // itag 17 = 144p
  // itag 36 = 240p
  // itag 18 = 360p
  // itag 22 = 720p
  // Algumas APIs marcam 144p/240p como 3GP/3GPP, mas o PS3 pode tocar.
  return (
    type.includes('mp4') ||
    mime.includes('mp4') ||
    type.includes('3gpp') ||
    mime.includes('3gpp') ||
    type.includes('3gp') ||
    mime.includes('3gp') ||
    itag === '17' ||
    itag === '36' ||
    itag === '18' ||
    itag === '22'
  );
}

function safeHost(rawUrl) {
  try {
    return new URL(rawUrl).host;
  } catch(e) {
    return '';
  }
}

function redirectToVideo(result) {
  const headers = {
    'Location': result.url,
    ...corsHeaders()
  };

  if (result && result.selected) {
    headers['X-PS3-Video-Resolution'] = String(result.selected.qualityLabel || result.selected.height || '');
    headers['X-PS3-Video-Bitrate'] = String(result.selected.bitrate || '');
    headers['X-PS3-Video-Risk'] = String(result.selected.bitrateRisk || 'unknown');
    headers['X-PS3-Video-Host'] = String(result.selected.host || '');
  }

  return new Response(null, {
    status: 302,
    headers
  });
}


async function proxyVideo(videoUrl, request) {
  try {
    const upstreamHeaders = new Headers();

    upstreamHeaders.set('User-Agent', request.headers.get('User-Agent') || 'Mozilla/5.0');
    upstreamHeaders.set('Accept', request.headers.get('Accept') || '*/*');
    upstreamHeaders.set('Accept-Language', request.headers.get('Accept-Language') || 'en-US,en;q=0.9');

    const range = request.headers.get('Range');
    if (range) {
      upstreamHeaders.set('Range', range);
    }

    const videoResponse = await fetch(videoUrl, {
      method: request.method === 'HEAD' ? 'HEAD' : 'GET',
      headers: upstreamHeaders,
      redirect: 'follow'
    });

    const newHeaders = new Headers(videoResponse.headers);

    newHeaders.set('Access-Control-Allow-Origin', '*');
    newHeaders.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    newHeaders.set('Access-Control-Allow-Headers', 'Range, Content-Type, User-Agent, Accept, Accept-Language');
    newHeaders.set('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Accept-Ranges, Content-Type');
    newHeaders.set('Accept-Ranges', 'bytes');

    if (!newHeaders.get('Content-Type')) {
      newHeaders.set('Content-Type', 'video/mp4');
    }

    newHeaders.delete('content-security-policy');
    newHeaders.delete('x-content-type-options');
    newHeaders.delete('x-frame-options');
    newHeaders.delete('cross-origin-resource-policy');
    newHeaders.delete('cross-origin-opener-policy');

    return new Response(videoResponse.body, {
      status: videoResponse.status,
      statusText: videoResponse.statusText,
      headers: newHeaders
    });

  } catch (error) {
    throw new Error('Falha no proxy: ' + error.message);
  }
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function PLAYER_HTML(videoId, origin, options) {
  const safeVideoId = escapeHtml(videoId);
  const query = `mode=${encodeURIComponent(options.mode)}&quality=${encodeURIComponent(options.quality)}${options.strict ? '&strict=1' : ''}`;
  const videoUrl = `${origin}/video/${safeVideoId}.mp4?${query}`;
  const flashFile = encodeURIComponent(videoUrl);

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>PS3 Flash Player Test</title>
</head>
<body style="background:#000;color:#fff;text-align:center;margin:0;padding-top:30px;font-family:Arial;">
  <h3 style="margin:0 0 10px 0;">PS3 Flash Player Test</h3>
  <p style="font-size:12px;color:#aaa;">Mode: ${escapeHtml(options.mode)} | Quality: ${escapeHtml(options.quality)} | Strict: ${options.strict ? 'yes' : 'no'}</p>

  <script type="text/javascript" src="https://github.com/PS3-Pro/Pages/raw/main/resources/scripts/flash_objects.js"></script>

  <div id="player_936" align="center">
    <script type="text/javascript">
      var flashvars_936 = {};
      var params_936 = {
        quality: "low",
        wmode: "transparent",
        bgcolor: "#000000",
        allowScriptAccess: "always",
        allowFullScreen: "true",
        flashvars: "fichier=${flashFile}&auto_play=true&apercu="
      };
      var attributes_936 = {};

      flashObject(
        "https://github.com/PS3-Pro/Pages/raw/main/resources/swf/video_player/video_player_27.swf",
        "player_936",
        "960",
        "540",
        "8",
        false,
        flashvars_936,
        params_936,
        attributes_936
      );
    </script>
  </div>

  <p style="font-size:12px;color:#888;word-break:break-all;">fichier=${escapeHtml(videoUrl)}</p>

  <p>
    <a style="color:#66ccff;" href="/player?v=${safeVideoId}&mode=ps3">PS3 Auto</a>
    |
    <a style="color:#66ccff;" href="/player?v=${safeVideoId}&mode=ps3&quality=144p&strict=1">144p</a>
    |
    <a style="color:#66ccff;" href="/player?v=${safeVideoId}&mode=ps3&quality=240p&strict=1">240p</a>
    |
    <a style="color:#66ccff;" href="/player?v=${safeVideoId}&mode=ps3&quality=360p&strict=1">360p</a>
    |
    <a style="color:#66ccff;" href="/player?v=${safeVideoId}&mode=ps3&quality=480p&strict=1">480p</a>
    |
    <a style="color:#66ccff;" href="/player?v=${safeVideoId}&mode=ps3&quality=720p&strict=1">720p</a>
    |
    <a style="color:#66ccff;" href="/debug?v=${safeVideoId}&mode=ps3&quality=${escapeHtml(options.quality)}${options.strict ? '&strict=1' : ''}">Debug</a>
  </p>
</body>
</html>`;
}

const HTML_PAGE = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>YouTube PS3 Worker</title>
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: radial-gradient(circle at top, #202852 0, #101429 42%, #070914 100%);
      color: #fff;
      font-family: Arial, sans-serif;
      text-align: center;
      padding: 34px 18px;
    }
    h1 {
      margin: 0 0 8px 0;
      color: #fff;
      font-size: 30px;
      letter-spacing: 1px;
    }
    .sub {
      margin: 0 0 24px 0;
      color: #9ca3af;
      font-size: 13px;
    }
    .box {
      width: 100%;
      max-width: 760px;
      margin: 16px auto;
      padding: 22px;
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.16);
      border-radius: 12px;
      box-shadow: 0 12px 34px rgba(0,0,0,0.35);
    }
    .label {
      color: #cbd5e1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 1px;
      margin-bottom: 10px;
      font-weight: bold;
    }
    .id {
      color: #ffe162;
      font-weight: bold;
      margin-bottom: 16px;
      font-size: 15px;
    }
    .row {
      display: flex;
      gap: 10px;
      justify-content: center;
      flex-wrap: wrap;
    }
    a, button {
      display: inline-block;
      min-width: 132px;
      padding: 13px 18px;
      color: #fff;
      text-decoration: none;
      border: 0;
      border-radius: 7px;
      font-size: 15px;
      font-weight: bold;
      cursor: pointer;
      font-family: Arial, sans-serif;
    }
    .ps3 { background: #007bff; }
    .pc { background: #dc2626; }
    .health { background: #16a34a; }
    .debug { background: #4b5563; }
    a:hover, button:hover { filter: brightness(1.15); }
    input {
      width: 100%;
      max-width: 360px;
      padding: 13px 14px;
      margin: 0 0 14px 0;
      border: 1px solid rgba(255,255,255,0.25);
      border-radius: 7px;
      background: #050816;
      color: #fff;
      font-size: 16px;
      text-align: center;
      outline: none;
    }
    input:focus { border-color: #66ccff; }
    .info {
      max-width: 760px;
      margin: 22px auto 0 auto;
      color: #9ca3af;
      font-size: 12px;
      line-height: 1.7;
    }
    code {
      background: rgba(0,0,0,0.5);
      color: #e5e7eb;
      padding: 2px 6px;
      border-radius: 4px;
    }
  </style>
</head>
<body>
  <h1>YouTube PS3 Worker</h1>
  <p class="sub">MP4 progressive test page · Dynamic APIs</p>

  <div class="box">
    <div class="label">Default test video</div>
    <div class="id">7H6swK9OHC0</div>

    <div class="row">
      <a class="ps3" href="/player?v=7H6swK9OHC0&mode=ps3">TEST PS3</a>
      <a class="pc" href="/video/7H6swK9OHC0.mp4?mode=pc&quality=best">TEST PC</a>
      <a class="health" href="/health?v=7H6swK9OHC0">HEALTH</a>
      <a class="debug" href="/debug?v=7H6swK9OHC0&mode=ps3">DEBUG</a>
    </div>
  </div>

  <div class="box">
    <div class="label">Test another video</div>

    <form action="/go" method="get">
      <input id="videoIdInput" name="v" type="text" maxlength="160" placeholder="YouTube video ID or URL">

      <div class="row">
        <button class="ps3" type="submit" name="type" value="ps3">TEST PS3</button>
        <button class="pc" type="submit" name="type" value="pc">TEST PC</button>
        <button class="health" type="submit" name="type" value="health">HEALTH</button>
        <button class="debug" type="submit" name="type" value="debug">DEBUG</button>
      </div>
    </form>
  </div>

  <div class="info">
    <p><code>TEST PS3</code> abre o Flash player usando <code>/video/ID.mp4?mode=ps3</code>.</p>
    <p><code>TEST PC</code> abre direto o melhor MP4 progressivo para PC.</p>
    <p><code>HEALTH</code> mostra quais APIs Invidious estão vivas agora.</p>
    <p>A lista de APIs vem de <code>api.invidious.io</code> + fallback fixo.</p>
  </div>
</body>
</html>`;

function ERROR_HTML(videoId, reason) {
  const safeVideoId = escapeHtml(videoId);
  const safeReason = escapeHtml(reason || 'Não foi possível extrair o vídeo.');

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Erro</title>
</head>
<body style="background:#1a1a2e;color:#fff;text-align:center;padding:50px;font-family:Arial;">
  <h1 style="color:#ff0000;">⚠ API Indisponível</h1>
  <p>Não foi possível extrair o vídeo <strong>${safeVideoId}</strong></p>
  <p>${safeReason}</p>
  <br>
  <a href="/" style="background:#ff0000;color:#fff;padding:15px 30px;text-decoration:none;border-radius:5px;">← Voltar</a>
</body>
</html>`;
}
