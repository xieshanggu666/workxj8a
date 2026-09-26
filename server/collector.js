import crypto from 'node:crypto'
import { db } from './db.js'
import { ingestPost, now } from './pipeline.js'

// ===== 采集调度参数（演示用小时间窗） =====
export const TICK_MS = 3000            // 调度扫描间隔（与通知调度器一致）
export const RETRY_BASE_MS = 10000    // 拉取失败退避基数（第 n 轮等待 n × 基数）
const FETCH_TIMEOUT_MS = 8000         // rss/api 拉取超时
const TYPES_TEXT = { mock: '模拟信息流', rss: 'RSS/Atom', api: 'HTTP JSON' }
export const DS_TYPES = TYPES_TEXT
export const DS_STATUS = {
  running: '采集中', retrying: '退避重试', success: '成功', failed: '已失败', idle: '空闲'
}
export const RUN_STATUS = { running: '采集中', retrying: '退避重试', success: '成功', failed: '失败' }

const q = (sql, ...p) => db.prepare(sql).all(...p)
const q1 = (sql, ...p) => db.prepare(sql).get(...p)
const run = (sql, ...p) => db.prepare(sql).run(...p)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const sleepTick = () => new Promise((r) => setImmediate(r))

function safeParse(s, dft) { try { const v = JSON.parse(s || ''); return v ?? dft } catch { return dft } }

function addLog(sourceId, runId, action, detail, operator = '系统') {
  run('INSERT INTO collect_logs (source_id,run_id,action,detail,operator,time) VALUES (?,?,?,?,?,?)',
    sourceId, runId, action, detail || '', operator, now())
}

// ===== 数据源连接器（mock / rss / api） =====
// 统一返回：{ items: [{ ext_id, title, content, topic?, media?, published? }], cursor }

// 模拟信息流：按偏移量游标稳定生成舆情（含命中既有预警规则的内容）；
// endpoint 含 flaky → 首轮拉取模拟瞬时故障（验证退避重试）；含 always-fail → 持续失败（验证达上限 → 手动重试）。
const MOCK_FEEDS = {
  'food-safety': {
    topic: '食品安全', media: '食安监测流',
    titles: [
      ['又一门店被曝后厨卫生隐患', '暗访发现部分门店后厨卫生操作不规范，状况堪忧；消费者投诉无人处理、客服回应迟缓，网友拍摄视频广泛传播并质疑监管缺位，呼吁严查整改并退款赔偿。'],
      ['品牌回应后厨卫生问题并称全面自查', '针对后厨卫生投诉与退款维权，涉事品牌深夜回应称已关停相关门店、启动全面自查并邀请第三方复查，但网友质疑回应迟缓，舆论不满持续发酵。'],
      ['市场监管部门进驻检查食品安全', '针对集中投诉与卫生质疑，监管部门通报已进驻现场检查食品安全情况，消费者不满此前回应迟缓，要求尽快退款并公布抽检结果。'],
      ['消费者投诉餐品中发现异物', '多位消费者投诉在餐品中吃出异物，卫生堪忧，要求退款和赔偿；门店回应迟缓引发不满，相关维权视频在社交平台刷屏。'],
      ['连锁餐饮卫生评级结果公布', '最新卫生评级结果公布，部分门店评级偏低被要求限期整改，消费者投诉其管理混乱、回应迟缓，质疑日常卫生监督缺位，退款诉求集中。'],
      ['网红餐厅排队卫生问题再惹争议', '有博主拍摄网红餐厅后厨脏乱视频，卫生问题再惹争议，老顾客吐槽体验差、客服回应迟缓，投诉与退款维权话题热度攀升。']
    ]
  },
  'flaky-complaints': {
    topic: '服务投诉', media: '投诉监测流',
    titles: [
      ['预售商品迟迟不发货投诉量激增', '大量用户投诉支付定金后预售商品迟迟不发货，物流停滞，客服回应迟缓，吐槽刷屏，不少人要求退款并质疑虚假宣传。'],
      ['自动扣款引发集中投诉', '平台自动扣款规则不清晰引发集中投诉，用户不满且质疑刻意误导，纷纷要求退款；客服回应迟缓、故障频出使矛盾升级。'],
      ['客服响应慢投诉再次升温', '近期客服响应慢的投诉再次升温，多位用户等待多日仍未解决，故障推诿、回应迟缓令用户不满，退款维权讨论扩散。'],
      ['促销活动被投诉虚假宣传', '多名消费者投诉促销存在虚假宣传嫌疑，实际优惠与宣传不符，下单流程繁琐、系统故障频发，用户要求退款并吐槽回应迟缓。'],
      ['会员自动续费投诉扎堆', '会员自动续费扣款提醒缺失，投诉扎堆出现，网友吐槽退款流程繁琐、客服回应迟缓，质疑平台故意设置障碍，不满情绪扩散。'],
      ['物流停滞多日无人处理', '快递物流停滞多日无人处理，用户反复催单、投诉未果，客服回应迟缓还遭遇系统故障，维权与退款诉求居高不下。']
    ]
  }
}
// 未注册标识的 mock 源使用通用模板（保证新建自定义 mock 源也可演练）
const GENERIC_FEED = {
  topic: '网络监测', media: '模拟信息流',
  titles: [
    ['消费者投诉服务流程繁琐', '多位用户投诉业务办理流程繁琐、回应迟缓，要求尽快优化并给出退款方案。'],
    ['产品质量问题引发网友吐槽', '有网友吐槽产品存在明显质量瑕疵，售后渠道响应不及时，负面讨论增多。'],
    ['官方回应争议并称已启动核查', '针对持续发酵的争议，官方回应称已启动核查，后续进展将及时公布。'],
    ['延期交付问题再被提及', '项目延期交付的老问题再被用户提及，维权声音集中，舆论关注度上升。']
  ]
}

// 演练故障判定：含 always-fail → 每轮都失败；含 flaky → 仅在该源「从未成功过」的首轮尝试失败一次（重试即恢复，之后永久恢复）
function injectionFailure(ds, attempt) {
  if (ds.endpoint.includes('always-fail')) return '数据源持续不可达（模拟故障）'
  if (ds.endpoint.includes('flaky')) {
    const everOk = q1("SELECT 1 FROM collect_runs WHERE source_id=? AND status='success' LIMIT 1", ds.id)
    if (!everOk && attempt <= 1) return '数据源瞬时不可达（模拟，重试可恢复）'
  }
  return null
}

function mockFetch(ds, cfg, attempt) {
  const inj = injectionFailure(ds, attempt)
  if (inj) throw new Error(inj)
  const key = String(ds.endpoint || '').replace(/^mock:\/\//, '').trim()
  const feed = MOCK_FEEDS[key] || GENERIC_FEED
  const offset = Number(ds.cursor || 0)
  const batch = Math.max(1, ds.batch || 3)
  const overlap = Math.max(0, Math.min(batch, Number(cfg.overlap || 0))) // 每轮混入的重复条数（演示去重）
  const items = []
  const total = feed.titles.length
  // 首批回溯（offset=0）无历史可重叠，全部按新增处理；后续轮次末尾 overlap 条回放上一批尾部 ext_id（相同幂等键，演示跨轮去重）
  const effOverlap = offset > 0 ? overlap : 0
  const freshN = batch - effOverlap
  for (let i = 0; i < batch; i++) {
    let seq
    if (i < freshN) seq = offset + i                     // 新增区
    else seq = offset - 1 - (i - freshN)                 // 回放区：上一批尾部 offset-1、offset-2…
    const idx = ((seq % total) + total) % total
    const t = feed.titles[idx]
    items.push({
      ext_id: `m${seq}`,
      title: t[0], content: t[1], topic: feed.topic, media: feed.media,
      published: new Date(Date.now() - (batch - i) * 60000).toISOString()
    })
  }
  // 游标按实际新增条数推进（回放上批的 overlap 条不占新位置）
  return { items, cursor: String(offset + freshN) }
}

// RSS / Atom 极简解析（无第三方依赖，演示用）
function decodeEntities(s) {
  return String(s || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'").replace(/&amp;/g, '&')
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n))
    .replace(/\s+/g, ' ').trim()
}
function tag(xml, name) {
  const m = xml.match(new RegExp(`<${name}[^>]*>([\\s\\S]*?)</${name}>`, 'i'))
  return m ? decodeEntities(m[1]) : ''
}

async function fetchText(url) {
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS)
  try {
    const r = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': 'pubmon-collector/1.0' } })
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
    return await r.text()
  } finally { clearTimeout(timer) }
}

function parseRss(xml, ds, cfg) {
  const batch = Math.max(1, ds.batch || 10)
  const map = Object.assign({ title: 'title', content: 'description', topic: '', media: '', published: 'pubDate', id: 'guid' }, safeParse(cfg.mapping, {}))
  const nodes = xml.match(/<(?:item|entry)[\s>][\s\S]*?<\/(?:item|entry)>/gi) || []
  const raw = nodes.map((node) => {
    const t = tag(node, map.title) || tag(node, 'title')
    const content = tag(node, map.content) || tag(node, 'description') || tag(node, 'summary')
    const pubStr = tag(node, map.published) || tag(node, 'pubDate') || tag(node, 'updated')
    const pubMs = pubStr ? Date.parse(pubStr) : NaN
    const linkM = node.match(/<link[^>]*href=["']([^"']+)["']/i) || node.match(/<link[^>]*>([\s\S]*?)<\/link>/i)
    const ext = tag(node, map.id) || tag(node, 'guid') || (linkM && linkM[1]) || t
    return {
      ext_id: String(ext || crypto.randomUUID()).slice(0, 200),
      title: t.slice(0, 200), content: content.slice(0, 1000),
      topic: map.topic ? tag(node, map.topic) : '',
      media: map.media ? tag(node, map.media) : '',
      published: Number.isNaN(pubMs) ? null : new Date(pubMs).toISOString(),
      _pubMs: Number.isNaN(pubMs) ? 0 : pubMs
    }
  }).filter((x) => x.title && x.content)
  raw.sort((a, b) => b._pubMs - a._pubMs)
  const cursorMs = Number(ds.cursor || 0)
  let list = cursorMs ? raw.filter((x) => x._pubMs > cursorMs) : raw.slice(0, batch)
  list = list.slice(0, batch).map(({ _pubMs, ...rest }) => rest)
  const nextMs = raw.length ? raw[0]._pubMs : cursorMs
  return { items: list, cursor: String(nextMs || cursorMs || '') }
}

// HTTP JSON 源：cfg.fetch_path 指向条目数组（如 data.items），cfg.mapping 自定义字段
async function apiFetch(ds, cfg, attempt) {
  const inj = injectionFailure(ds, attempt)
  if (inj) throw new Error(inj)
  const txt = await fetchText(ds.endpoint)
  let body
  try { body = JSON.parse(txt) } catch { throw new Error('响应不是合法 JSON') }
  const map = Object.assign({ list: 'items', title: 'title', content: 'content', topic: 'topic', media: 'media', published: 'published', id: 'id' }, safeParse(cfg.mapping, {}))
  const arr = String(map.list || 'items').split('.').filter(Boolean).reduce((o, k) => (o == null ? o : o[k]), body)
  if (!Array.isArray(arr)) throw new Error(`未在响应路径「${map.list}」找到条目数组`)
  const batch = Math.max(1, ds.batch || 10)
  const cursorMs = Number(ds.cursor || 0)
  const items = arr.slice(0, batch * 2).map((x) => {
    const get = (k) => (k ? String(k).split('.').reduce((o, kk) => (o == null ? o : o[kk]), x) : '')
    const pubRaw = get(map.published)
    const pubMs = pubRaw ? Date.parse(pubRaw) : NaN
    return {
      ext_id: String(get(map.id) || crypto.randomUUID()).slice(0, 200),
      title: String(get(map.title) || '').trim().slice(0, 200),
      content: String(get(map.content) || '').trim().slice(0, 1000),
      topic: String(get(map.topic) || '').trim(),
      media: String(get(map.media) || '').trim(),
      published: pubRaw && !Number.isNaN(pubMs) ? new Date(pubMs).toISOString() : null,
      _pubMs: Number.isNaN(pubMs) ? 0 : pubMs
    }
  }).filter((x) => x.title && x.content)
  items.sort((a, b) => b._pubMs - a._pubMs)
  let list = cursorMs ? items.filter((x) => x._pubMs > cursorMs) : items.slice(0, batch)
  list = list.slice(0, batch).map(({ _pubMs, ...rest }) => rest)
  const nextMs = items.length ? items[0]._pubMs : cursorMs
  return { items: list, cursor: String(nextMs || cursorMs || '') }
}

async function fetchSource(ds, attempt) {
  const cfg = safeParse(ds.config, {}) || {}
  if (ds.type === 'mock') return mockFetch(ds, cfg, attempt)
  if (ds.type === 'api') return apiFetch(ds, cfg, attempt)
  const inj = injectionFailure(ds, attempt)
  if (inj) throw new Error(inj)
  const xml = await fetchText(ds.endpoint)
  return parseRss(xml, ds, cfg)
}

// ===== 配置校验 =====
export function validateDatasource(b, { partial = false } = {}) {
  if (!b || typeof b !== 'object') return '参数格式错误'
  if (!partial || b.name !== undefined) {
    if (typeof b.name !== 'string' || !b.name.trim()) return '数据源名称必填'
  }
  if (!partial || b.type !== undefined) {
    if (!TYPES_TEXT[b.type]) return '数据源类型无效（mock/rss/api）'
  }
  if (!partial || b.endpoint !== undefined) {
    if (typeof b.endpoint !== 'string' || !b.endpoint.trim()) return '连接地址必填'
  }
  if (b.source_id !== undefined) {
    const sid = +b.source_id
    if (!q1('SELECT 1 FROM sources WHERE id=?', sid)) return '归属渠道不存在'
  }
  if (b.interval_sec !== undefined && !(+b.interval_sec >= 5)) return '采集周期至少 5 秒'
  if (b.batch !== undefined && !(+b.batch >= 1 && +b.batch <= 200)) return '单轮条数需在 1–200 之间'
  if (b.max_retry !== undefined && !(+b.max_retry >= 0 && +b.max_retry <= 5)) return '重试上限需在 0–5 之间'
  if (b.config !== undefined) {
    if (typeof b.config === 'string') { try { JSON.parse(b.config) } catch { return '高级参数 config 不是合法 JSON' } }
    else if (typeof b.config !== 'object') return '高级参数 config 格式错误'
  }
  return null
}

function normalizeCfg(b) {
  let cfg = {}
  if (b.config && typeof b.config === 'object') cfg = b.config
  else if (typeof b.config === 'string' && b.config.trim()) { try { cfg = JSON.parse(b.config) } catch { cfg = {} } }
  return JSON.stringify(cfg)
}

export function createDatasource(b, actor) {
  const ts = now()
  const r = run(`INSERT INTO datasources
    (name,type,endpoint,source_id,interval_sec,batch,max_retry,config,enabled,collect_active,last_trigger,created,created_by,updated)
    VALUES (?,?,?,?,?,?,?,?,?,0,'manual',?,?,?)`,
    b.name.trim(), b.type, b.endpoint.trim(), +b.source_id || 1,
    Math.max(5, +b.interval_sec || 30), Math.min(200, Math.max(1, +b.batch || 5)),
    Math.min(5, Math.max(0, +b.max_retry ?? 3)), normalizeCfg(b),
    b.enabled === false ? 0 : 1, ts, actor.user, ts)
  const id = Number(r.lastInsertRowid)
  addLog(id, null, 'edit', `管理员新建数据源「${b.name.trim()}」（${TYPES_TEXT[b.type]}）`, actor.user)
  return getDatasource(id)
}

export function updateDatasource(id, b, actor) {
  const ds = q1('SELECT * FROM datasources WHERE id=?', id)
  if (!ds) return null
  const next = {
    name: typeof b.name === 'string' && b.name.trim() ? b.name.trim() : ds.name,
    type: TYPES_TEXT[b.type] ? b.type : ds.type,
    endpoint: typeof b.endpoint === 'string' && b.endpoint.trim() ? b.endpoint.trim() : ds.endpoint,
    source_id: b.source_id !== undefined && q1('SELECT 1 FROM sources WHERE id=?', +b.source_id) ? +b.source_id : ds.source_id,
    interval_sec: b.interval_sec !== undefined ? Math.max(5, +b.interval_sec || ds.interval_sec) : ds.interval_sec,
    batch: b.batch !== undefined ? Math.min(200, Math.max(1, +b.batch || ds.batch)) : ds.batch,
    max_retry: b.max_retry !== undefined ? Math.min(5, Math.max(0, +b.max_retry ?? ds.max_retry)) : ds.max_retry,
    config: b.config !== undefined ? normalizeCfg(b) : ds.config
  }
  run(`UPDATE datasources SET name=?,type=?,endpoint=?,source_id=?,interval_sec=?,batch=?,max_retry=?,config=?,updated=? WHERE id=?`,
    next.name, next.type, next.endpoint, next.source_id, next.interval_sec, next.batch, next.max_retry, next.config, now(), id)
  addLog(id, null, 'edit', `管理员更新数据源配置：${describeDiff(ds, next)}`, actor.user)
  return getDatasource(id)
}

function describeDiff(before, next) {
  const fields = [['name', '名称'], ['type', '类型'], ['endpoint', '地址'], ['interval_sec', '周期'], ['batch', '单轮条数'], ['max_retry', '重试上限']]
  const diffs = fields.filter(([k]) => String(before[k]) !== String(next[k])).map(([, t]) => t)
  return diffs.length ? `调整 ${diffs.join('、')}` : '调整高级参数'
}

export function deleteDatasource(id, actor) {
  const ds = q1('SELECT * FROM datasources WHERE id=?', id)
  if (!ds) return null
  run('DELETE FROM collect_logs WHERE source_id=?', id)
  run('DELETE FROM collect_runs WHERE source_id=?', id)
  run('DELETE FROM datasources WHERE id=?', id)
  addLog(id, null, 'edit', `管理员删除数据源「${ds.name}」（历史采集日志一并清除）`, actor.user)
  return true
}

// 连接启用/停用（admin）：停用同时停止采集任务
export function toggleDatasourceEnabled(id, actor) {
  const ds = q1('SELECT * FROM datasources WHERE id=?', id)
  if (!ds) return null
  const next = ds.enabled ? 0 : 1
  if (!next) {
    run('UPDATE datasources SET enabled=0, collect_active=0, next_run_at=NULL, updated=? WHERE id=?', now(), id)
    addLog(id, null, 'stop', '管理员停用连接，采集任务一并停止', actor.user)
  } else {
    run('UPDATE datasources SET enabled=1, updated=? WHERE id=?', now(), id)
    addLog(id, null, 'edit', '管理员启用连接（采集任务保持停止，需值班员启动）', actor.user)
  }
  return getDatasource(id)
}

// ===== 采集任务启停（ops 值班员） =====
export function startCollect(id, actor, { immediate = true, reason = 'start' } = {}) {
  const ds = q1('SELECT * FROM datasources WHERE id=?', id)
  if (!ds) return null
  if (!ds.enabled) return { error: '连接已停用，请先由管理员启用' }
  run('UPDATE datasources SET collect_active=1, fail_count=0, last_trigger=?, next_run_at=?, updated=? WHERE id=?',
    reason, immediate ? Date.now() : Date.now() + ds.interval_sec * 1000, now(), id)
  addLog(id, null, 'start', immediate ? '值班员启动采集任务（立即拉取一轮）' : '值班员启动采集任务（按周期调度）', actor.user)
  return getDatasource(id)
}

export function stopCollect(id, actor) {
  const ds = q1('SELECT * FROM datasources WHERE id=?', id)
  if (!ds) return null
  run('UPDATE datasources SET collect_active=0, next_run_at=NULL, updated=? WHERE id=?', now(), id)
  // 进行中的运行保留其结果（采集中状态由运行收尾自然更新）；retrying 运行标记为取消失败，避免悬挂
  run("UPDATE collect_runs SET status='failed', error=? WHERE source_id=? AND status='retrying'", '采集任务已手动停止', id)
  addLog(id, null, 'stop', '值班员停止采集任务（进行中的拉取完成后不再调度）', actor.user)
  return getDatasource(id)
}

// 立即采集（手动触发；失败状态下相当于手动重试——重置退避）
export function runNow(id, actor) {
  const ds = q1('SELECT * FROM datasources WHERE id=?', id)
  if (!ds) return null
  if (!ds.enabled) return { error: '连接已停用，无法采集' }
  run('UPDATE datasources SET collect_active=1, fail_count=0, last_trigger=?, next_run_at=?, updated=? WHERE id=?',
    'manual', Date.now(), now(), id)
  addLog(id, null, 'run', actor.role === 'admin' ? '管理员手动触发立即采集' : '值班员手动触发立即采集', actor.user)
  return getDatasource(id)
}

// 游标重置（重新回溯；不影响启停状态，下一轮按新游标采集）
export function resetCursor(id, actor) {
  const ds = q1('SELECT * FROM datasources WHERE id=?', id)
  if (!ds) return null
  run('UPDATE datasources SET cursor=?, cursor_at=NULL, fail_count=0, updated=? WHERE id=?', '', now(), id)
  addLog(id, null, 'reset', `重置增量游标（原游标：${ds.cursor || '空'}），下一轮重新回溯`, actor.user)
  return getDatasource(id)
}

// ===== 读取模型 =====
function decorate(ds) {
  if (!ds) return null
  const runnable = !!ds.enabled && !!ds.collect_active
  let statusText = ds.enabled ? (runnable ? (ds.last_status || '等待调度') : '已停止') : '已停用'
  if (runnable) statusText = ds.last_status === 'failed' ? DS_STATUS.failed : ds.last_status === 'success' ? DS_STATUS.success : ds.last_status === 'retrying' ? DS_STATUS.retrying : '等待调度'
  return {
    ...ds,
    config_obj: safeParse(ds.config, {}),
    type_text: TYPES_TEXT[ds.type] || ds.type,
    status_text: statusText,
    runnable,
    due: runnable && ds.next_run_at != null && ds.next_run_at <= Date.now()
  }
}
export function getDatasource(id) { return decorate(q1('SELECT * FROM datasources WHERE id=?', id)) }
export function listDatasources() { return q('SELECT * FROM datasources ORDER BY id').map(decorate) }

export function overview() {
  const list = listDatasources()
  const counts = {
    total: list.length,
    active: list.filter((d) => d.runnable).length,
    enabled: list.filter((d) => d.enabled).length,
    failed: list.filter((d) => d.last_status === 'failed').length,
    retrying: list.filter((d) => d.last_status === 'retrying').length,
    due: list.filter((d) => d.due).length
  }
  const agg = q1(`SELECT COALESCE(SUM(total_runs),0) runs, COALESCE(SUM(total_fetched),0) fetched,
    COALESCE(SUM(total_inserted),0) inserted, COALESCE(SUM(total_duplicate),0) dup,
    COALESCE(SUM(total_failed),0) failed FROM datasources`)
  return { list, counts, totals: agg, types: TYPES_TEXT, statusText: DS_STATUS }
}

export function listRuns({ sourceId = null, limit = 50 } = {}) {
  const rows = sourceId
    ? q('SELECT * FROM collect_runs WHERE source_id=? ORDER BY id DESC LIMIT ?', sourceId, limit)
    : q(`SELECT cr.*, d.name source_name, d.type source_type FROM collect_runs cr
        LEFT JOIN datasources d ON d.id=cr.source_id ORDER BY cr.id DESC LIMIT ?`, limit)
  return rows.map((r) => ({ ...r, status_text: RUN_STATUS[r.status] || r.status }))
}
export function listLogs({ sourceId = null, limit = 100 } = {}) {
  const rows = sourceId
    ? q('SELECT * FROM collect_logs WHERE source_id=? ORDER BY id DESC LIMIT ?', sourceId, limit)
    : q(`SELECT cl.*, d.name source_name FROM collect_logs cl
        LEFT JOIN datasources d ON d.id=cl.source_id ORDER BY cl.id DESC LIMIT ?`, limit)
  return rows
}

// ===== 调度器：扫描到期数据源 → 拉取 → 游标/去重/重试/闭环 =====
const inflight = new Set()

async function runSource(dsId, trigger) {
  let ds = q1('SELECT * FROM datasources WHERE id=?', dsId)
  if (!ds || inflight.has(dsId)) return
  inflight.add(dsId)
  // 复用最近一条未完成的运行（retrying 到期补采），attempts 连续递增；成功/失败的历史行不重开
  let runRow = q1("SELECT * FROM collect_runs WHERE source_id=? AND status IN ('running','retrying') ORDER BY id DESC LIMIT 1", dsId)
  let isNewRun = false
  const ts0 = now()
  if (runRow) {
    run("UPDATE collect_runs SET status='running', attempts=attempts+1, updated=? WHERE id=?", ts0, runRow.id)
    runRow = q1('SELECT * FROM collect_runs WHERE id=?', runRow.id)
  } else {
    const rr = run(`INSERT INTO collect_runs (source_id,triggered_by,status,attempts,cursor_before,started,updated)
      VALUES (?,?, 'running',1,?,?,?)`, dsId, trigger, ds.cursor, ts0, ts0)
    runRow = q1('SELECT * FROM collect_runs WHERE id=?', Number(rr.lastInsertRowid))
    isNewRun = true
  }
  run("UPDATE datasources SET last_run_at=?, last_status='running', last_error='', last_trigger=?, updated=? WHERE id=?",
    ts0, trigger, ts0, dsId)
  const runId = runRow.id
  try {
    const fetched = await fetchSource(ds, runRow.attempts)
    await sleepTick() // 条目处理前让出事件循环，保证采集期间 API 可响应
    let inserted = 0, duplicate = 0, failed = 0, alertsFired = 0, crisesCreated = 0, crisesMerged = 0
    for (const it of fetched.items) {
      const idemKey = `ds${dsId}:${it.ext_id}`
      try {
        const r = ingestPost(
          { title: it.title, content: it.content, topic: it.topic, media: it.media, source_id: ds.source_id },
          { idemKey, published: it.published || null }
        )
        if (r.duplicate) duplicate++
        else {
          inserted++
          for (const t of r.triggered || []) {
            alertsFired++
            if (t.crisisId && !t.deduped) crisesCreated++
            if (t.deduped) crisesMerged++
          }
        }
      } catch (e) {
        // 条目级失败不拖垮整轮：记录条数，下轮游标前的未收录条目可回溯（rss/api 游标只在全成功时推进）
        failed++
        addLog(dsId, runId, 'failed', `条目《${it.title}》处理失败：${String(e.message || e)}`)
      }
    }
    // 游标推进策略：mock 始终推进（其游标为生成偏移）；rss/api 仅在无条目失败时推进，避免漏采
    const nextCursor = ds.type === 'mock' || failed === 0 ? String(fetched.cursor ?? ds.cursor) : ds.cursor
    const ts = now()
    db.exec('BEGIN')
    try {
      run(`UPDATE collect_runs SET status='success', fetched=?, inserted=?, duplicate=?, failed=?,
        alerts_fired=?, crises_created=?, crises_merged=?, cursor_after=?, error='', finished=?, updated=? WHERE id=?`,
        fetched.items.length, inserted, duplicate, failed, alertsFired, crisesCreated, crisesMerged, nextCursor, ts, ts, runId)
      run(`UPDATE datasources SET cursor=?, cursor_at=?, fail_count=0, last_status='success', last_error='',
        total_runs=total_runs+1, total_fetched=total_fetched+?, total_inserted=total_inserted+?,
        total_duplicate=total_duplicate+?, next_run_at=?, last_trigger='scheduler', updated=? WHERE id=?`,
        nextCursor, ts, fetched.items.length, inserted, duplicate,
        Date.now() + ds.interval_sec * 1000, ts, dsId)
      db.exec('COMMIT')
    } catch (e) { try { db.exec('ROLLBACK') } catch { /* 已回滚 */ }; throw e }
    addLog(dsId, runId, 'success',
      `采集成功：拉取 ${fetched.items.length} 条，新增 ${inserted}，去重 ${duplicate}${failed ? `，失败 ${failed}` : ''}` +
      `${alertsFired ? `，触发预警 ${alertsFired}` : ''}${crisesCreated ? `，新建危机 ${crisesCreated}` : ''}${crisesMerged ? `，并入危机 ${crisesMerged}` : ''}`)
  } catch (e) {
    const msg = String(e.message || e)
    const attempts = runRow.attempts || 1
    const maxRetry = Math.max(0, ds.max_retry || 0)
    const giveUp = attempts > maxRetry
    const ts = now()
    db.exec('BEGIN')
    try {
      run("UPDATE collect_runs SET attempts=?, status=?, error=?, updated=? WHERE id=?",
        attempts, giveUp ? 'failed' : 'retrying', msg, ts, runId)
      if (giveUp) {
        run(`UPDATE datasources SET fail_count=?, last_status='failed', last_error=?,
          total_runs=total_runs+1, total_failed=total_failed+1, next_run_at=NULL, updated=? WHERE id=?`,
          attempts, msg, ts, dsId)
      } else {
        const wait = RETRY_BASE_MS * attempts
        run(`UPDATE datasources SET fail_count=?, last_status='retrying', last_error=?, next_run_at=?, updated=? WHERE id=?`,
          attempts, msg, Date.now() + wait, ts, dsId)
      }
      db.exec('COMMIT')
    } catch (err) { try { db.exec('ROLLBACK') } catch { /* 已回滚 */ }; throw err }
    addLog(dsId, runId, giveUp ? 'failed' : 'retry', giveUp
      ? `第 ${attempts} 次拉取失败，已达重试上限（${maxRetry} 次），任务停止调度，可手动重试：${msg}`
      : `第 ${attempts} 次拉取失败：${msg}，${(RETRY_BASE_MS * attempts) / 1000} 秒后自动重试`)
  } finally {
    // 成功/失败后安排下一个周期（停止/停用以数据库状态为准）
    const cur = q1('SELECT enabled,collect_active,last_status FROM datasources WHERE id=?', dsId)
    if (cur && cur.enabled && cur.collect_active && cur.last_status === 'success') {
      const d2 = q1('SELECT interval_sec FROM datasources WHERE id=?', dsId)
      run('UPDATE datasources SET last_trigger=?, next_run_at=? WHERE id=?',
        'scheduler', Date.now() + d2.interval_sec * 1000, dsId)
    }
    inflight.delete(dsId)
  }
}

// 调度一轮：到期且运行中的数据源各派发一次（单源单飞）
export async function runCollectTick() {
  const nowMs = Date.now()
  const due = q(`SELECT id, last_trigger FROM datasources
    WHERE enabled=1 AND collect_active=1 AND next_run_at IS NOT NULL AND next_run_at<=? ORDER BY id`, nowMs)
  for (const d of due) {
    if (inflight.has(d.id)) continue
    // 派发后立即清空到期时间，失败重试路径会重新设置退避时间，避免同一轮重复派发
    run('UPDATE datasources SET next_run_at=NULL WHERE id=? AND next_run_at<=?', d.id, nowMs)
    runSource(d.id, d.last_trigger || 'scheduler').catch((e) => console.error('[COLLECT] 采集异常：', e.message))
  }
}

let timer = null
export function startCollector() {
  if (timer) return
  timer = setInterval(() => { runCollectTick().catch((e) => console.error('[COLLECT] 调度异常：', e.message)) }, TICK_MS)
  if (timer.unref) timer.unref()
  console.log(`[COLLECT] 采集调度器已启动（每 ${TICK_MS / 1000} 秒扫描：到期拉取 / 失败退避重试 / 游标增量去重）`)
}
export function stopCollector() { if (timer) clearInterval(timer); timer = null }

// 进程启动恢复：上次运行中被打断（崩溃/重启）的运行标记失败，由值班员决定是否重新启动采集；
// collect_active=1 的任务保留运行状态并立即补采一轮（游标保证不重复）
export function recoverCollect() {
  const stale = q("SELECT id FROM collect_runs WHERE status IN ('running','retrying')")
  for (const r of stale) {
    run("UPDATE collect_runs SET status='failed', error=?, updated=? WHERE id=?",
      '服务曾中断，本次采集未完成', now(), r.id)
    run("UPDATE datasources SET last_status='failed', last_error=? WHERE id=?", '服务曾中断，采集未完成', r.id)
  }
  const actives = q('SELECT id FROM datasources WHERE enabled=1 AND collect_active=1')
  for (const d of actives) run('UPDATE datasources SET next_run_at=?, last_trigger=? WHERE id=?', Date.now(), 'start', d.id)
  return { stale: stale.length, active: actives.length }
}
