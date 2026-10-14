// Neighbor endorsements for Dorado Proven Providers.
// Only providers already on the list (ids in ids.json, Watch List excluded) can be endorsed.
// Storage: Upstash Redis connected through Vercel (Storage > Upstash for Redis).
// GET  /api/endorsements            -> { "<provider-id>": [{by,t,at}], ... }
// POST /api/endorsements {id,by,t}  -> adds one endorsement; it shows on the site right away
const IDS = new Set(require('./ids.json'));
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
// Push notification to John for every new post (ntfy app, subscribe to this topic)
const NTFY_TOPIC = process.env.NTFY_TOPIC || 'dpp-endorse-2b9b85eef5de';

async function notify(id, by, t) {
  try {
    await fetch('https://ntfy.sh/' + NTFY_TOPIC, {
      method: 'POST',
      headers: { Title: 'New endorsement posted', Click: 'https://doradoprovenproviders.com/#p/' + id, Tags: 'thumbsup' },
      body: by + ' endorsed ' + id + ':\n' + (t || '(no comment)') + '\nhttps://doradoprovenproviders.com/#p/' + id,
    });
  } catch (e) { /* posting still succeeds if the notification fails */ }
}

async function redis(cmds) {
  const r = await fetch(URL_ + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error('storage ' + r.status);
  return (await r.json()).map(x => x.result);
}

const clean = s => String(s || '').replace(/[\u0000-\u001f\u007f<>]+/g, ' ').replace(/\s+/g, ' ').trim();

async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') { try { return JSON.parse(req.body); } catch { return {}; } }
  let raw = '';
  for await (const c of req) { raw += c; if (raw.length > 10000) break; }
  try { return JSON.parse(raw); } catch { return {}; }
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (!URL_ || !TOKEN) return res.status(503).json({ error: 'not_configured' });
  try {
    if (req.method === 'GET') {
      const [ids] = await redis([['SMEMBERS', 'endorse:ids']]);
      const live = (ids || []).filter(id => IDS.has(id));
      if (!live.length) return res.status(200).json({});
      const lists = await redis(live.map(id => ['LRANGE', 'endorse:' + id, '0', '-1']));
      const out = {};
      live.forEach((id, i) => {
        const items = (lists[i] || []).map(s => { try { return JSON.parse(s); } catch { return null; } })
          .filter(x => x && x.by).map(x => ({ by: clean(x.by), t: clean(x.t), at: x.at }));
        if (items.length) out[id] = items;
      });
      return res.status(200).json(out);
    }
    if (req.method === 'POST') {
      const b = await readBody(req);
      if (b.website) return res.status(200).json({ ok: true }); // honeypot field only bots fill in
      const id = clean(b.id).toLowerCase();
      const by = clean(b.by).slice(0, 60);
      const t = clean(b.t).slice(0, 600);
      if (!IDS.has(id)) return res.status(400).json({ error: 'bad_provider' });
      if (by.length < 2) return res.status(400).json({ error: 'name_required' });
      if (/(https?:\/\/|www\.|\.com\b)/i.test(by + ' ' + t)) return res.status(400).json({ error: 'no_links' });
      const ip = String(req.headers['x-forwarded-for'] || 'x').split(',')[0].trim();
      const [count] = await redis([['INCR', 'rl:' + ip], ['EXPIRE', 'rl:' + ip, '3600']]);
      if (count > 6) return res.status(429).json({ error: 'too_many' });
      const item = JSON.stringify({ by, t, at: new Date().toISOString() });
      await redis([['RPUSH', 'endorse:' + id, item], ['SADD', 'endorse:ids', id]]);
      await notify(id, by, t);
      return res.status(200).json({ ok: true });
    }
    res.setHeader('Allow', 'GET, POST');
    return res.status(405).json({ error: 'method' });
  } catch (e) {
    return res.status(502).json({ error: 'storage_error' });
  }
};
