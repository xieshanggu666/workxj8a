<template>
  <div class="collect">
    <div class="ctoolbar">
      <div class="subtabs">
        <button v-for="t in subtabs" :key="t.key" :class="{active:sub===t.key}" @click="sub=t.key">
          {{ t.icon }} {{ t.label }}
          <span v-if="t.key==='sources' && warnCount" class="bd">{{ warnCount }}</span>
        </button>
      </div>
      <span class="me">👤 {{ store.user.name }} · {{ roleText(store.user.role) }}</span>
    </div>

    <!-- ============ 数据源 ============ -->
    <template v-if="sub==='sources'">
      <div class="summary">
        <span class="sum">数据源 <b>{{ counts.total || 0 }}</b></span>
        <span class="sum ok">采集中 <b>{{ counts.active || 0 }}</b></span>
        <span class="sum warn">退避重试 <b>{{ counts.retrying || 0 }}</b></span>
        <span class="sum err">已失败 <b>{{ counts.failed || 0 }}</b></span>
        <span class="sum">累计运行 <b>{{ totals.runs || 0 }}</b></span>
        <span class="sum">累计拉取 <b>{{ totals.fetched || 0 }}</b></span>
        <span class="sum ok">新增入库 <b>{{ totals.inserted || 0 }}</b></span>
        <span class="sum">去重 <b>{{ totals.dup || 0 }}</b></span>
      </div>

      <!-- 新建数据源（管理员） -->
      <div class="card" v-if="isAdmin">
        <h4>🔌 新增数据源连接 <small class="ro">仅管理员可配置；值班员负责启停采集</small></h4>
        <form class="ds-form" @submit.prevent="addSource">
          <div class="row">
            <input v-model="form.name" placeholder="数据源名称，如 食安舆情模拟流" required />
            <select v-model="form.type">
              <option v-for="(txt,k) in types" :key="k" :value="k">{{ txt }}</option>
            </select>
            <select v-model.number="form.source_id">
              <option v-for="s in store.sources" :key="s.id" :value="s.id">归属渠道：{{ s.name }}</option>
            </select>
          </div>
          <input v-model="form.endpoint" :placeholder="endpointHint" required />
          <div class="row">
            <label>周期(秒)<input v-model.number="form.interval_sec" type="number" min="5" /></label>
            <label>单轮条数<input v-model.number="form.batch" type="number" min="1" max="200" /></label>
            <label>失败重试上限<input v-model.number="form.max_retry" type="number" min="0" max="5" /></label>
            <label class="en"><input type="checkbox" v-model="form.enabled" /> 连接启用</label>
          </div>
          <input v-model="form.config" :placeholder="configHint" />
          <button class="save" type="submit">保存数据源</button>
          <p class="hint">💡 {{ formHint }}</p>
        </form>
      </div>

      <div v-if="!sources.length" class="none">暂无数据源，由管理员新增连接</div>
      <div class="ds-grid">
        <div v-for="s in sources" :key="s.id" class="ds" :class="{off:!s.enabled,running:s.runnable,failed:s.last_status==='failed'}">
          <div class="ds-head">
            <span class="ds-type">{{ s.type_text }}</span>
            <b class="ds-name">{{ s.name }}</b>
            <span class="st" :class="statusClass(s)">{{ s.status_text }}</span>
            <span v-if="s.runnable" class="live"><i></i>调度中</span>
          </div>
          <div class="ds-url"><span>{{ s.endpoint }}</span></div>
          <div class="ds-meta">
            <span>渠道 <i>{{ srcName(s.source_id) }}</i></span>
            <span>周期 <i>{{ s.interval_sec }}s</i></span>
            <span>每轮 <i>{{ s.batch }} 条</i></span>
            <span>重试 <i>{{ s.max_retry }} 次</i></span>
            <span>游标 <i :title="s.cursor_at || ''">{{ s.cursor || '（空·首次回溯）' }}</i></span>
          </div>
          <div class="ds-meta">
            <span>运行 <i>{{ s.total_runs }}</i></span>
            <span>拉取 <i>{{ s.total_fetched }}</i></span>
            <span class="ok">新增 <i>{{ s.total_inserted }}</i></span>
            <span>去重 <i>{{ s.total_duplicate }}</i></span>
            <span v-if="s.fail_count" class="err">连失 <i>{{ s.fail_count }}</i></span>
            <span v-if="s.last_run_at">最近 <i>{{ s.last_run_at }}</i></span>
          </div>
          <div v-if="s.last_error" class="ds-err">⚠ {{ s.last_error }}</div>
          <div class="ds-actions">
            <template v-if="canOps">
              <button v-if="!s.collect_active" class="op start" :disabled="!s.enabled" @click="act(s,'start')">▶ 启动采集</button>
              <button v-else class="op stop" @click="act(s,'stop')">⏸ 停止采集</button>
              <button class="op" :disabled="!s.enabled" @click="act(s,'run')">⚡ 立即采集{{ s.last_status==='failed' ? '/重试' : '' }}</button>
              <button class="op" :disabled="!s.enabled" @click="act(s,'reset')">↺ 游标重置</button>
            </template>
            <button class="op log" @click="toggleDetail(s)">{{ detailId===s.id ? '收起记录' : '📜 运行记录' }}</button>
            <template v-if="isAdmin">
              <button class="op edit" @click="editSource(s)">✎ 编辑</button>
              <label class="switch" :title="s.enabled?'停用连接':'启用连接'">
                <input type="checkbox" :checked="!!s.enabled" @change="act(s,'toggle')" />
                <span></span>
              </label>
              <button class="op del" @click="del(s)">删除</button>
            </template>
          </div>
          <div v-if="detailId===s.id" class="ds-detail">
            <div class="dd-col">
              <h5>采集运行（最近 {{ detail.runs.length }} 次）</h5>
              <div v-if="!detail.runs.length" class="none sm">暂无</div>
              <div v-for="r in detail.runs" :key="r.id" class="run-row" :class="r.status">
                <span class="st sm" :class="r.status">{{ r.status_text }}</span>
                <span class="r-trigger">{{ triggerText(r.triggered_by) }}</span>
                <span class="r-nums">尝试{{ r.attempts }} · 拉{{ r.fetched }} · 新{{ r.inserted }} · 重{{ r.duplicate }}<template v-if="r.failed"> · 失败{{ r.failed }}</template></span>
                <em>{{ r.finished || r.started }}</em>
                <small v-if="r.alerts_fired">🚨预警{{ r.alerts_fired }}<template v-if="r.crises_created"> · 新建危机{{ r.crises_created }}</template><template v-if="r.crises_merged"> · 并入{{ r.crises_merged }}</template></small>
                <small v-if="r.error" class="r-err">⚠ {{ r.error }}</small>
              </div>
            </div>
            <div class="dd-col">
              <h5>操作留痕</h5>
              <div v-if="!detail.logs.length" class="none sm">暂无</div>
              <div v-for="l in detail.logs.slice(0,15)" :key="l.id" class="log-row">
                <span class="lg-act" :class="l.action">{{ logText(l.action) }}</span>
                <span class="lg-detail">{{ l.detail }}</span>
                <em>{{ l.operator }} · {{ l.time }}</em>
              </div>
            </div>
          </div>
        </div>
      </div>
    </template>

    <!-- ============ 全局采集记录 ============ -->
    <template v-else>
      <div class="card">
        <h4>🧾 采集运行记录</h4>
        <div v-if="!runs.length" class="none">暂无采集运行</div>
        <table class="rtable">
          <thead><tr><th>#</th><th>数据源</th><th>触发</th><th>状态</th><th>尝试</th><th>拉取</th><th>新增</th><th>去重</th><th>闭环</th><th>开始</th></tr></thead>
          <tbody>
            <tr v-for="r in runs" :key="r.id">
              <td>{{ r.id }}</td>
              <td>{{ r.source_name || ('源#'+r.source_id) }}</td>
              <td>{{ triggerText(r.triggered_by) }}</td>
              <td><span class="st sm" :class="r.status">{{ r.status_text }}</span></td>
              <td>{{ r.attempts }}</td>
              <td>{{ r.fetched }}</td>
              <td class="ok">{{ r.inserted }}</td>
              <td>{{ r.duplicate }}</td>
              <td class="loop">
                <span v-if="r.alerts_fired">🚨{{ r.alerts_fired }}</span>
                <span v-if="r.crises_created">🆕危机{{ r.crises_created }}</span>
                <span v-if="r.crises_merged">↘并入{{ r.crises_merged }}</span>
                <small v-if="r.error" class="r-err">{{ r.error }}</small>
              </td>
              <td><em>{{ r.started }}</em></td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="card">
        <h4>📜 采集历史追踪</h4>
        <div v-if="!logs.length" class="none">暂无留痕</div>
        <div v-for="l in logs" :key="l.id" class="log-row lg-global">
          <span class="lg-act" :class="l.action">{{ logText(l.action) }}</span>
          <div class="lg-body">
            <b>{{ l.source_name || ('数据源#'+l.source_id) }}</b>
            <span>{{ l.detail }}</span>
          </div>
          <em>{{ l.operator }}<br />{{ l.time }}</em>
        </div>
      </div>
    </template>

    <!-- 编辑弹窗 -->
    <div v-if="editing" class="modal-mask" @click.self="editing=null">
      <div class="modal">
        <h4>✎ 编辑数据源「{{ editing.name }}」</h4>
        <div class="row"><input v-model="editing.name" placeholder="名称" /></div>
        <div class="row">
          <select v-model="editing.type">
            <option v-for="(txt,k) in types" :key="k" :value="k">{{ txt }}</option>
          </select>
          <select v-model.number="editing.source_id">
            <option v-for="s in store.sources" :key="s.id" :value="s.id">渠道：{{ s.name }}</option>
          </select>
        </div>
        <div class="row"><input v-model="editing.endpoint" placeholder="连接地址" /></div>
        <div class="row">
          <label>周期(秒)<input v-model.number="editing.interval_sec" type="number" min="5" /></label>
          <label>单轮条数<input v-model.number="editing.batch" type="number" min="1" max="200" /></label>
          <label>重试上限<input v-model.number="editing.max_retry" type="number" min="0" max="5" /></label>
        </div>
        <div class="row"><input v-model="editing._config" placeholder='高级参数 JSON，如 {"overlap":1}' /></div>
        <div class="modal-btns">
          <button class="op" @click="editing=null">取消</button>
          <button class="save" @click="saveEdit">保存</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { usePubStore } from '@/store/pub'

const store = usePubStore()
const sub = ref('sources')
const subtabs = [
  { key: 'sources', icon: '🕸️', label: '数据源与采集' },
  { key: 'runs', icon: '🧾', label: '采集记录' }
]
const sources = ref([])
const counts = ref({})
const totals = ref({})
const types = ref({})
const runs = ref([])
const logs = ref([])
const detailId = ref(null)
const detail = ref({ runs: [], logs: [] })
const editing = ref(null)

const blank = () => ({
  name: '', type: 'mock', endpoint: '', source_id: 1, interval_sec: 30,
  batch: 5, max_retry: 3, config: '', enabled: true
})
const form = ref(blank())

const isAdmin = computed(() => store.user.role === 'admin')
const canOps = computed(() => ['admin', 'ops'].includes(store.user.role))
const warnCount = computed(() => (counts.value.failed || 0) + (counts.value.retrying || 0))

const endpointHint = computed(() => ({
  mock: 'mock://food-safety（内置食安流；含 flaky / always-fail 可演练故障重试）',
  rss: 'https://example.com/feed.xml（RSS 2.0 / Atom）',
  api: 'https://example.com/api/posts（返回 JSON，字段路径在 config 中映射）'
}[form.value.type]))
const configHint = computed(() => ({
  mock: '高级参数 JSON（可留空）：{"overlap":1} 每轮回放 1 条已采集内容，演示跨轮去重',
  rss: '高级参数 JSON（可留空）：{"mapping":{"title":"title","content":"description","published":"pubDate","id":"guid"}}',
  api: '高级参数 JSON：{"list":"data.items","mapping":{"id":"id","title":"title","content":"content","topic":"topic","media":"media","published":"published"}}'
}[form.value.type]))

const HINT_TEXT = {
  mock: 'mock:// 模拟信息流，地址含 flaky 首轮瞬时故障（演示自动重试）、含 always-fail 持续失败（演到达上限后手动重试）；config 可配 {"overlap":1} 每轮回放 1 条旧内容演示去重。',
  rss: 'RSS/Atom 订阅地址，游标按条目发布时间增量推进；config 可配字段映射，如 {"mapping":{"content":"description","published":"pubDate","id":"guid"}}。',
  api: 'HTTP JSON 接口；config 需配条目数组路径与字段映射，如 {"list":"data.items","mapping":{"id":"id","title":"title","content":"content","published":"published"}}，支持点号路径。'
}
const formHint = computed(() => HINT_TEXT[form.value.type])

function roleText(r) { return { admin: '管理员', ops: '值班员', viewer: '观察员' }[r] || r }
function srcName(id) { const s = store.sources.find((x) => x.id === id); return s ? s.name : '#' + id }
function triggerText(t) { return { scheduler: '周期调度', manual: '手动触发', start: '启动补采' }[t] || t }
function logText(a) {
  return { start: '启动', stop: '停止', run: '立即采集', retry: '退避重试', success: '成功', failed: '失败', reset: '游标重置', edit: '配置变更' }[a] || a
}
function statusClass(s) {
  if (!s.enabled) return 'off'
  if (s.last_status === 'failed') return 'failed'
  if (s.last_status === 'retrying') return 'retrying'
  if (s.last_status === 'success') return 'success'
  return 'idle'
}

async function loadOverview() {
  const d = await store.fetchCollectOverview()
  sources.value = d.list
  counts.value = d.counts
  totals.value = d.totals
  types.value = d.types
  if (detailId.value) loadDetail(detailId.value, true)
}
async function loadDetail(id, silent = false) {
  const d = await store.fetchCollectSource(id)
  detail.value = { runs: d.runs, logs: d.logs }
  if (!silent) detailId.value = id
}
async function toggleDetail(s) {
  if (detailId.value === s.id) { detailId.value = null; detail.value = { runs: [], logs: [] }; return }
  detailId.value = s.id
  await loadDetail(s.id, true)
}
async function loadRunsLogs() {
  runs.value = await store.fetchCollectRuns()
  logs.value = await store.fetchCollectLogs()
}

async function addSource() {
  try {
    const body = { ...form.value }
    if (!body.config.trim()) delete body.config
    await store.saveDatasource(body)
    form.value = blank()
    await loadOverview()
  } catch (e) { store.msg(e.message, 'warn') }
}
async function act(s, op) {
  try {
    if (op === 'toggle') { await store.toggleDatasource(s.id) }
    else if (op === 'start') await store.startCollect(s.id)
    else if (op === 'stop') await store.stopCollect(s.id)
    else if (op === 'run') await store.runCollect(s.id)
    else if (op === 'reset') {
      if (!confirm(`重置「${s.name}」增量游标？下轮将从头回溯，幂等去重保证不产生重复舆情。`)) return
      await store.resetCollectCursor(s.id)
    }
    await Promise.all([loadOverview(), store.load()])
  } catch (e) { store.msg(e.message, 'warn') }
}
function editSource(s) {
  editing.value = { ...s, _config: s.config }
}
async function saveEdit() {
  const e = editing.value
  try {
    await store.updateDatasource(e.id, {
      name: e.name, type: e.type, endpoint: e.endpoint, source_id: e.source_id,
      interval_sec: e.interval_sec, batch: e.batch, max_retry: e.max_retry, config: e._config
    })
    editing.value = null
    await loadOverview()
  } catch (err) { store.msg(err.message, 'warn') }
}
async function del(s) {
  if (!confirm(`删除数据源「${s.name}」？其采集运行与留痕将一并清除（已采集舆情保留）。`)) return
  try { await store.delDatasource(s.id); await loadOverview() } catch (e) { store.msg(e.message, 'warn') }
}

let timer = null
onMounted(async () => {
  await loadOverview().catch(() => {})
  await loadRunsLogs().catch(() => {})
  timer = setInterval(() => {
    loadOverview().catch(() => {})
    if (sub.value === 'runs') loadRunsLogs().catch(() => {})
  }, 4000)
})
onUnmounted(() => clearInterval(timer))
</script>

<style scoped>
.collect{display:flex;flex-direction:column;gap:12px;}
.ctoolbar{display:flex;align-items:center;gap:12px;flex-wrap:wrap;}
.subtabs{display:flex;gap:6px;flex-wrap:wrap;}
.subtabs button{background:#13233f;border:1px solid rgba(120,160,220,0.2);color:#aebadd;padding:8px 14px;border-radius:8px;cursor:pointer;font-size:13px;position:relative;font-family:inherit;}
.subtabs button.active{background:linear-gradient(135deg,#00695c,#2962ff);color:#fff;border-color:transparent;}
.bd{position:absolute;top:-4px;right:-4px;background:#ef5350;color:#fff;font-size:9px;border-radius:8px;padding:1px 5px;font-weight:700;}
.me{margin-left:auto;font-size:11px;color:#8ba2c8;background:#13233f;border:1px solid rgba(120,160,220,0.2);border-radius:8px;padding:6px 12px;}
.summary{display:flex;gap:8px;flex-wrap:wrap;}
.sum{font-size:11px;color:#8ba2c8;background:#0f1b38;border:1px solid rgba(120,160,220,0.16);border-radius:14px;padding:5px 12px;}
.sum b{color:#dbe4f3;margin-left:4px;}
.sum.ok b{color:#81c784;} .sum.warn b{color:#ffcc80;} .sum.err b{color:#ef9a9a;}
.card{background:#0f1b38;border:1px solid rgba(120,160,220,0.16);border-radius:12px;padding:16px;}
h4{margin:0 0 12px;color:#fff;font-size:14px;}
h4 small{font-size:10px;color:#5b6f94;font-weight:400;margin-left:8px;}
h5{margin:0 0 8px;color:#aebadd;font-size:12px;}
.ds-form{display:flex;flex-direction:column;gap:8px;}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;}
.row input,.row select{flex:1;min-width:120px;}
.row label{display:flex;align-items:center;gap:5px;font-size:11px;color:#8ba2c8;flex:1;min-width:110px;}
.row label input{flex:1;min-width:60px;}
.row label.en{flex:0 0 auto;}
.row label.en input{width:auto;flex:none;}
input,select,button{font-family:inherit;background:#0f1b38;border:1px solid rgba(120,160,220,0.2);color:#dbe4f3;border-radius:8px;padding:8px 10px;font-size:12px;}
.save{background:#2962ff;border:none;color:#fff;font-weight:600;cursor:pointer;}
.hint{margin:0;font-size:10px;color:#5b6f94;line-height:1.6;}
.ds-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:12px;}
.ds{background:#0f1b38;border:1px solid rgba(120,160,220,0.16);border-left:4px solid #546e7a;border-radius:10px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;}
.ds.running{border-left-color:#42a5f5;}
.ds.failed{border-left-color:#ef5350;}
.ds.off{opacity:.6;}
.ds-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap;}
.ds-type{font-size:10px;background:#0d2137;color:#90caf9;border-radius:5px;padding:2px 8px;}
.ds-name{color:#fff;font-size:13px;flex:1;min-width:120px;}
.live{font-size:10px;color:#81c784;display:flex;align-items:center;gap:4px;}
.live i{width:6px;height:6px;border-radius:50%;background:#66bb6a;animation:pulse 1.4s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.ds-url{font-size:10px;color:#5b6f94;word-break:break-all;background:#0c1730;border-radius:6px;padding:4px 8px;}
.ds-meta{display:flex;gap:12px;flex-wrap:wrap;font-size:10px;color:#5b6f94;}
.ds-meta i{color:#90caf9;font-style:normal;margin-left:2px;}
.ds-meta .ok i{color:#81c784;} .ds-meta .err i{color:#ef9a9a;}
.st{font-size:10px;padding:2px 9px;border-radius:6px;flex:none;}
.st.sm{font-size:9px;padding:1px 7px;}
.st.idle,.st.pending,.st.running{background:#0d2137;color:#90caf9;}
.st.success{background:#1b5e20;color:#a5d6a7;}
.st.retrying{background:#3e2723;color:#ffcc80;}
.st.failed{background:#4a1518;color:#ef9a9a;}
.st.off{background:#263238;color:#b0bec5;}
.ds-err{font-size:10px;color:#ef9a9a;background:#2c1418;border-radius:6px;padding:4px 8px;}
.ds-actions{display:flex;gap:6px;flex-wrap:wrap;align-items:center;}
.op{background:none;border:1px solid rgba(144,202,249,.4);color:#90caf9;cursor:pointer;border-radius:7px;padding:4px 10px;font-size:11px;}
.op:disabled{opacity:.35;cursor:not-allowed;}
.op.start{border-color:rgba(102,187,106,.5);color:#81c784;}
.op.stop{border-color:rgba(255,138,101,.45);color:#ffab91;}
.op.edit{border-color:rgba(255,213,79,.4);color:#ffe082;}
.op.del{border-color:rgba(239,83,80,.4);color:#ef5350;}
.op.log{border-color:rgba(120,160,220,0.3);color:#8ba2c8;}
.switch{position:relative;width:36px;height:20px;display:inline-block;}
.switch input{opacity:0;width:0;height:0;}
.switch span{position:absolute;inset:0;background:#243357;border-radius:20px;transition:.2s;cursor:pointer;}
.switch span:before{content:'';position:absolute;width:16px;height:16px;left:2px;top:2px;background:#7b8db3;border-radius:50%;transition:.2s;}
.switch input:checked+span{background:#2962ff;}
.switch input:checked+span:before{transform:translateX(16px);background:#fff;}
.ds-detail{display:grid;grid-template-columns:1fr 1fr;gap:10px;border-top:1px dashed rgba(120,160,220,0.15);padding-top:10px;}
@media(max-width:720px){.ds-detail{grid-template-columns:1fr;}}
.dd-col{display:flex;flex-direction:column;gap:5px;max-height:240px;overflow-y:auto;}
.run-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:10px;color:#8ba2c8;background:#0c1730;border-radius:6px;padding:5px 8px;}
.run-row em{margin-left:auto;color:#5b6f94;font-style:normal;}
.run-row small{color:#ffcc80;}
.run-row small.r-err{color:#ef9a9a;flex-basis:100%;}
.r-trigger{color:#aebadd;}
.r-nums{color:#8ba2c8;}
.log-row{display:flex;align-items:baseline;gap:8px;font-size:10px;color:#8ba2c8;padding:4px 0;}
.log-row em{margin-left:auto;color:#5b6f94;font-style:normal;white-space:nowrap;}
.lg-act{flex:none;font-size:9px;padding:1px 7px;border-radius:5px;background:#16263f;color:#90caf9;border:1px solid rgba(144,202,249,.25);}
.lg-act.start,.lg-act.success{color:#81c784;border-color:rgba(102,187,106,.35);}
.lg-act.stop,.lg-act.reset{color:#b0bec5;border-color:rgba(176,190,197,.3);}
.lg-act.retry{color:#ffe082;border-color:rgba(255,213,79,.35);}
.lg-act.failed{color:#ffab91;border-color:rgba(255,138,101,.35);}
.lg-detail{flex:1;}
.rtable{width:100%;border-collapse:collapse;font-size:11px;}
.rtable th{color:#5b6f94;font-weight:500;text-align:left;padding:6px 8px;border-bottom:1px solid rgba(120,160,220,0.15);font-size:10px;}
.rtable td{padding:7px 8px;border-bottom:1px solid rgba(120,160,220,0.08);color:#aebadd;}
.rtable em{color:#5b6f94;font-style:normal;}
.rtable .ok{color:#81c784;}
.loop{display:flex;gap:6px;flex-wrap:wrap;}
.loop small.r-err{color:#ef9a9a;flex-basis:100%;}
.lg-global{align-items:flex-start;padding:8px 0;border-bottom:1px dashed rgba(120,160,220,0.1);}
.lg-body{flex:1;min-width:0;}
.lg-body b{color:#dbe4f3;font-size:11px;display:block;}
.lg-body span{color:#8ba2c8;font-size:10px;}
.lg-global em{text-align:right;}
.modal-mask{position:fixed;inset:0;background:rgba(4,10,22,.7);z-index:60;display:grid;place-items:center;}
.modal{background:#0f1b38;border:1px solid rgba(120,160,220,0.3);border-radius:12px;padding:18px;width:min(560px,92vw);display:flex;flex-direction:column;gap:10px;}
.modal .row input,.modal .row select{flex:1;}
.modal-btns{display:flex;justify-content:flex-end;gap:8px;}
.none{color:#5b6f94;text-align:center;padding:24px;}
.none.sm{padding:8px;font-size:10px;}
</style>
