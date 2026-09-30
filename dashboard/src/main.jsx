import React, { useEffect, useMemo, useState } from 'react'
import { createRoot } from 'react-dom/client'
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  Radar,
  RadarChart,
  PolarAngleAxis,
  PolarGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import './styles.css'

const API = '/api'

const TYPE_LABELS = {
  appointment: 'Запись',
  sales: 'Продажа',
  delivery: 'Выдача / доставка',
  consultation: 'Консультация',
  completed_deal: 'Закрытая сделка',
  critical: 'Критический',
  not_classified: 'Не классифицирован',
}

const RISK_LABELS = {
  critical: 'Критический',
  warning: 'Предупреждение',
  normal: 'Норма',
  not_analyzed: 'Не проанализирован',
}

const CRITERIA_LABELS = {
  greeting: 'Приветствие',
  needs_discovery: 'Выявление потребности',
  urgency: 'Срочность',
  target_action: 'Целевое действие',
  objection_handling: 'Возражения',
  closing: 'Закрытие',
}

const DURATION_LABELS = {
  lt_1m: '< 1 мин',
  '1_3m': '1–3 мин',
  '3_5m': '3–5 мин',
  '5_10m': '5–10 мин',
  '10m_plus': '10+ мин',
  unknown: 'Нет данных',
}

function formatDateInput(date) {
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60000)
  return local.toISOString().slice(0, 10)
}

function initialFilters() {

  const end = new Date()
  const start = new Date()
  start.setDate(start.getDate() - 29)
  return {
    date_from: formatDateInput(start),
    date_to: formatDateInput(end),
    account: '',
    manager: '',
    direction: '',
    status: '',
    call_type: '',
    risk_level: '',
    score_min: '',
    score_max: '',
    duration_min: '',
    duration_max: '',
    has_transcription: '',
    has_quality: '',
    search: '',
  }
}


function formatDateTime(value) {
  if (!value) return '—'
  return new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit', month: '2-digit', year: '2-digit', hour: '2-digit', minute: '2-digit',
  }).format(new Date(value))
}

function formatDay(value) {
  if (!value) return ''
  return new Intl.DateTimeFormat('ru-RU', { day: '2-digit', month: '2-digit' }).format(new Date(value))
}

function formatDuration(seconds) {
  if (seconds == null || Number.isNaN(Number(seconds))) return '—'
  const total = Math.max(0, Math.round(Number(seconds)))
  const hours = Math.floor(total / 3600)
  const min = Math.floor((total % 3600) / 60)
  const sec = total % 60
  if (hours) return `${hours}ч ${min}м`
  return min ? `${min}м ${sec}с` : `${sec}с`
}

function buildQuery(filters, extras = {}) {
  const p = new URLSearchParams()
  Object.entries(filters).forEach(([key, value]) => {
    if (value === '' || value == null) return
    if (key === 'date_from') {
      p.set(key, new Date(`${value}T00:00:00`).toISOString())
      return
    }
    if (key === 'date_to') {
      const d = new Date(`${value}T00:00:00`)
      d.setDate(d.getDate() + 1)
      p.set(key, d.toISOString())
      return
    }
    p.set(key, String(value))
  })
  Object.entries(extras).forEach(([key, value]) => p.set(key, String(value)))
  return p.toString()
}

class ApiError extends Error {
  constructor(message, status) {
    super(message)
    this.status = status
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, { credentials: 'same-origin', ...options })
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`
    try {
      const body = await response.json()
      detail = body.detail || detail
    } catch (_) {}
    throw new ApiError(detail, response.status)
  }
  return response.json()
}

function Badge({ children, kind = 'neutral' }) {
  return <span className={`badge badge-${kind}`}>{children}</span>
}

function RiskBadge({ risk }) {
  if (!risk) return <Badge>—</Badge>
  return <Badge kind={risk}>{RISK_LABELS[risk] || risk}</Badge>
}

function Score({ value }) {
  if (value == null) return <span className="muted">—</span>
  const cls = value >= 80 ? 'good' : value >= 60 ? 'warn' : 'bad'
  return <span className={`score score-${cls}`}>{value}</span>
}

function Delta({ value, suffix = '%', inverse = false }) {
  if (value == null) return <span className="delta delta-neutral">нет сравнения</span>
  const numeric = Number(value)
  const good = numeric === 0 ? null : inverse ? numeric < 0 : numeric > 0
  const cls = good == null ? 'neutral' : good ? 'good' : 'bad'
  const sign = numeric > 0 ? '+' : ''
  return <span className={`delta delta-${cls}`}>{sign}{numeric}{suffix}</span>
}

function KpiCard({ label, value, hint, tone = 'default', delta, deltaSuffix = '%', inverseDelta = false }) {
  return (
    <div className={`kpi-card tone-${tone}`}>
      <div className="kpi-label-row"><div className="kpi-label">{label}</div>{delta !== undefined && <Delta value={delta} suffix={deltaSuffix} inverse={inverseDelta} />}</div>
      <div className="kpi-value">{value ?? '—'}</div>
      {hint && <div className="kpi-hint">{hint}</div>}
    </div>
  )
}

function Select({ value, onChange, children, ariaLabel }) {
  return <select aria-label={ariaLabel} value={value} onChange={(e) => onChange(e.target.value)}>{children}</select>
}

function LoginScreen({ onAuthenticated }) {
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const submit = async (event) => {
    event.preventDefault()
    setLoading(true)
    setError('')
    try {
      const result = await fetchJson(`${API}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })
      onAuthenticated(result)
    } catch (err) {
      setError(err.status === 401 ? 'Неверный логин или пароль' : err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="login-shell">
      <div className="login-glow" />
      <form className="login-card" onSubmit={submit}>
        <div className="login-brand"><span className="brand-mark">D</span><div><strong>DALEON</strong><span>CALL INTELLIGENCE</span></div></div>
        <div className="login-copy"><h1>Вход в аналитику</h1><p>Доступ к звонкам, транскриптам и записям разрешён только авторизованному администратору.</p></div>
        {error && <div className="error-banner">{error}</div>}
        <label className="login-field"><span>Логин</span><input autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} autoFocus /></label>
        <label className="login-field"><span>Пароль</span><input type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} /></label>
        <button className="btn btn-primary login-submit" disabled={loading || !username || !password}>{loading ? 'Проверка…' : 'Войти'}</button>
        <div className="login-security">HttpOnly session · SameSite Strict · серверная проверка каждого analytics-запроса</div>
      </form>
    </div>
  )
}

function DatePresets({ filters, setFilters, options }) {
  const applyDays = (days) => {
    const end = new Date()
    const start = new Date()
    start.setDate(start.getDate() - (days - 1))
    setFilters((prev) => ({ ...prev, date_from: formatDateInput(start), date_to: formatDateInput(end) }))
  }
  const applyAll = () => {
    const min = options.min_started_at ? formatDateInput(new Date(options.min_started_at)) : ''
    const max = options.max_started_at ? formatDateInput(new Date(options.max_started_at)) : formatDateInput(new Date())
    setFilters((prev) => ({ ...prev, date_from: min, date_to: max }))
  }
  return (
    <div className="preset-row">
      <span>Быстрый период:</span>
      <button className="chip" onClick={() => applyDays(1)}>Сегодня</button>
      <button className="chip" onClick={() => applyDays(7)}>7 дней</button>
      <button className="chip" onClick={() => applyDays(30)}>30 дней</button>
      <button className="chip" onClick={() => applyDays(90)}>90 дней</button>
      <button className="chip" onClick={applyAll}>Всё время</button>
      <span className="preset-range">{filters.date_from || '…'} → {filters.date_to || '…'}</span>
    </div>
  )
}

function Filters({ filters, setFilters, options, onReset, exportHref }) {
  const set = (key) => (value) => setFilters((prev) => ({ ...prev, [key]: value }))
  const active = Object.entries(filters).filter(([key, value]) => !['date_from', 'date_to'].includes(key) && value !== '' && value != null).length
  return (
    <section className="panel filters-panel">
      <div className="section-heading">
        <div><h2>Фильтры {active > 0 && <Badge kind="type">{active}</Badge>}</h2><p>Все KPI, графики, менеджеры и звонки относятся к одному срезу.</p></div>
        <div className="heading-actions"><a className="btn btn-ghost" href={exportHref}>↓ CSV</a><button className="btn btn-ghost" onClick={onReset}>Сбросить</button></div>
      </div>
      <DatePresets filters={filters} setFilters={setFilters} options={options} />
      <div className="filters-grid">
        <label><span>С даты</span><input type="date" value={filters.date_from} onChange={(e) => set('date_from')(e.target.value)} /></label>
        <label><span>По дату</span><input type="date" value={filters.date_to} onChange={(e) => set('date_to')(e.target.value)} /></label>
        <label><span>АТС / группа</span><Select ariaLabel="АТС" value={filters.account} onChange={set('account')}><option value="">Все</option>{options.accounts?.map((v) => <option key={v}>{v}</option>)}</Select></label>
        <label><span>Менеджер</span><Select ariaLabel="Менеджер" value={filters.manager} onChange={set('manager')}><option value="">Все</option>{options.managers?.map((v) => <option key={v}>{v}</option>)}</Select></label>
        <label><span>Тип звонка</span><Select ariaLabel="Тип звонка" value={filters.call_type} onChange={set('call_type')}><option value="">Все</option>{options.call_types?.map((v) => <option key={v} value={v}>{TYPE_LABELS[v] || v}</option>)}</Select></label>
        <label><span>Риск</span><Select ariaLabel="Риск" value={filters.risk_level} onChange={set('risk_level')}><option value="">Все</option>{options.risk_levels?.map((v) => <option key={v} value={v}>{RISK_LABELS[v] || v}</option>)}</Select></label>
        <label><span>Статус пайплайна</span><Select ariaLabel="Статус" value={filters.status} onChange={set('status')}><option value="">Все</option>{options.statuses?.map((v) => <option key={v}>{v}</option>)}</Select></label>
        <label><span>Направление</span><Select ariaLabel="Направление" value={filters.direction} onChange={set('direction')}><option value="">Все</option>{options.directions?.map((v) => <option key={v}>{v}</option>)}</Select></label>
        <label><span>Score от</span><input type="number" min="0" max="100" value={filters.score_min} onChange={(e) => set('score_min')(e.target.value)} placeholder="0" /></label>
        <label><span>Score до</span><input type="number" min="0" max="100" value={filters.score_max} onChange={(e) => set('score_max')(e.target.value)} placeholder="100" /></label>
        <label><span>Длительность от, сек</span><input type="number" min="0" value={filters.duration_min} onChange={(e) => set('duration_min')(e.target.value)} /></label>
        <label><span>Длительность до, сек</span><input type="number" min="0" value={filters.duration_max} onChange={(e) => set('duration_max')(e.target.value)} /></label>
        <label><span>Транскрипция</span><Select ariaLabel="Транскрипция" value={filters.has_transcription} onChange={set('has_transcription')}><option value="">Любая</option><option value="true">Есть</option><option value="false">Нет</option></Select></label>
        <label><span>Анализ качества</span><Select ariaLabel="Анализ качества" value={filters.has_quality} onChange={set('has_quality')}><option value="">Любой</option><option value="true">Есть</option><option value="false">Нет</option></Select></label>
        <label className="search-filter"><span>Поиск</span><input value={filters.search} onChange={(e) => set('search')(e.target.value)} placeholder="Телефон, ID, менеджер, текст разговора…" /></label>
      </div>
    </section>
  )
}

function Summary({ data }) {
  const s = data?.summary || {}
  const d = data?.comparison?.delta || {}
  return (
    <div className="kpi-grid kpi-grid-8">
      <KpiCard label="Всего звонков" value={s.total_calls ?? 0} hint={`${s.inbound_calls ?? 0} входящих · ${s.outbound_calls ?? 0} исходящих`} delta={d.total_calls_pct} />
      <KpiCard label="Средний score" value={s.avg_score ?? '—'} hint="из 100" tone={s.avg_score >= 80 ? 'good' : s.avg_score != null && s.avg_score < 60 ? 'bad' : 'default'} delta={d.avg_score} deltaSuffix=" п." />
      <KpiCard label="Анализ coverage" value={s.analysis_coverage != null ? `${s.analysis_coverage}%` : '—'} hint={`${s.analyzed_calls ?? 0} из ${s.total_calls ?? 0}`} delta={d.analysis_coverage_pp} deltaSuffix=" п.п." />
      <KpiCard label="Критические" value={s.critical_calls ?? 0} hint={`${s.critical_rate ?? '—'}% проанализированных`} tone="bad" delta={d.critical_calls_pct} inverseDelta />
      <KpiCard label="Закрытые сделки" value={s.completed_deal_calls ?? 0} hint={`Конверсия ${s.deal_rate ?? '—'}%`} tone="good" delta={d.completed_deal_calls_pct} />
      <KpiCard label="Записи" value={s.appointment_calls ?? 0} hint={`Доля ${s.appointment_rate ?? '—'}%`} />
      <KpiCard label="Ошибки пайплайна" value={s.pipeline_error_calls ?? 0} hint="failed status или error" tone={(s.pipeline_error_calls || 0) > 0 ? 'bad' : 'good'} />
      <KpiCard label="Средняя длительность" value={formatDuration(s.avg_duration_seconds)} hint={`Всего ${formatDuration(s.total_duration_seconds)}`} />
    </div>
  )
}

function PipelinePanel({ summary = {} }) {
  const total = summary.total_calls || 0
  const stages = [
    ['Звонки', summary.total_calls],
    ['Транскрипция', summary.transcribed_calls],
    ['Классификация', summary.classified_calls],
    ['Анализ', summary.analyzed_calls],
    ['Уведомление', summary.notified_calls],
  ]
  return (
    <section className="panel pipeline-panel">
      <div className="section-heading"><div><h2>Воронка обработки</h2><p>Сколько звонков прошло каждый этап текущего пайплайна.</p></div>{summary.pipeline_error_calls > 0 && <Badge kind="critical">{summary.pipeline_error_calls} ошибок</Badge>}</div>
      <div className="pipeline-stages">
        {stages.map(([label, value], index) => {
          const percent = total ? Math.round((Number(value || 0) * 1000) / total) / 10 : 0
          return <div className="pipeline-stage" key={label}><div className="pipeline-top"><span>{label}</span><strong>{value ?? 0}</strong></div><div className="pipeline-percent">{percent}%</div><div className="pipeline-track"><i style={{ width: `${Math.min(100, percent)}%` }} /></div>{index < stages.length - 1 && <span className="pipeline-arrow">→</span>}</div>
        })}
      </div>
    </section>
  )
}

function Charts({ data }) {
  const timeline = data?.timeline || []
  const risks = data?.risk_distribution || []
  const types = data?.call_type_distribution || []
  const criteria = (data?.criteria || []).map((x) => ({ ...x, label: CRITERIA_LABELS[x.key] || x.key, value: x.value ?? 0 }))
  const riskColors = ['#ef4444', '#f59e0b', '#22c55e', '#64748b']
  const weakest = [...criteria].filter((x) => x.value != null).sort((a, b) => a.value - b.value)[0]

  return (
    <div className="charts-grid">
      <section className="panel chart-wide">
        <div className="section-heading"><div><h2>Динамика</h2><p>Количество звонков и средний score по дням.</p></div></div>
        <div className="chart-box">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={timeline} margin={{ top: 10, right: 20, bottom: 0, left: -15 }}>
              <defs><linearGradient id="callsFill" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#60a5fa" stopOpacity={0.4}/><stop offset="95%" stopColor="#60a5fa" stopOpacity={0.02}/></linearGradient></defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#263349" />
              <XAxis dataKey="bucket" tickFormatter={formatDay} stroke="#8290a7" fontSize={12} />
              <YAxis yAxisId="left" stroke="#8290a7" fontSize={12} allowDecimals={false} />
              <YAxis yAxisId="right" orientation="right" domain={[0, 100]} stroke="#8290a7" fontSize={12} />
              <Tooltip contentStyle={{ background: '#111b2d', border: '1px solid #2a3a55', borderRadius: 10 }} labelFormatter={(v) => formatDateTime(v)} />
              <Area yAxisId="left" type="monotone" dataKey="calls" name="Звонки" stroke="#60a5fa" fill="url(#callsFill)" strokeWidth={2} />
              <Area yAxisId="right" type="monotone" dataKey="avg_score" name="Средний score" stroke="#a78bfa" fillOpacity={0} strokeWidth={2} connectNulls />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="panel">
        <div className="section-heading"><div><h2>Риски</h2><p>Распределение по уровню риска.</p></div></div>
        <div className="chart-box compact"><ResponsiveContainer width="100%" height="100%"><PieChart><Pie data={risks} dataKey="value" nameKey="name" innerRadius={52} outerRadius={78} paddingAngle={3}>{risks.map((entry, index) => <Cell key={entry.name} fill={riskColors[index % riskColors.length]} />)}</Pie><Tooltip formatter={(v, _n, p) => [v, RISK_LABELS[p.payload.name] || p.payload.name]} contentStyle={{ background: '#111b2d', border: '1px solid #2a3a55', borderRadius: 10 }} /></PieChart></ResponsiveContainer></div>
        <div className="legend-list">{risks.map((item, index) => <div key={item.name}><span className="legend-dot" style={{ background: riskColors[index % riskColors.length] }} />{RISK_LABELS[item.name] || item.name}<strong>{item.value}</strong></div>)}</div>
      </section>

      <section className="panel">
        <div className="section-heading"><div><h2>Типы звонков</h2><p>Классификация AI.</p></div></div>
        <div className="chart-box compact"><ResponsiveContainer width="100%" height="100%"><BarChart data={types.slice(0, 8)} layout="vertical" margin={{ left: 15, right: 10 }}><CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#263349" /><XAxis type="number" stroke="#8290a7" fontSize={12} allowDecimals={false} /><YAxis type="category" dataKey="name" tickFormatter={(v) => TYPE_LABELS[v] || v} width={115} stroke="#8290a7" fontSize={11} /><Tooltip formatter={(v) => [v, 'Звонков']} labelFormatter={(v) => TYPE_LABELS[v] || v} contentStyle={{ background: '#111b2d', border: '1px solid #2a3a55', borderRadius: 10 }} /><Bar dataKey="value" fill="#38bdf8" radius={[0, 5, 5, 0]} /></BarChart></ResponsiveContainer></div>
      </section>

      <section className="panel">
        <div className="section-heading"><div><h2>Критерии качества</h2><p>{weakest ? `Слабейший: ${weakest.label} — ${weakest.value}` : 'Средние оценки по компонентам.'}</p></div></div>
        <div className="chart-box compact"><ResponsiveContainer width="100%" height="100%"><RadarChart data={criteria} outerRadius="72%"><PolarGrid stroke="#334155" /><PolarAngleAxis dataKey="label" tick={{ fill: '#a8b4c8', fontSize: 10 }} /><Radar dataKey="value" stroke="#a78bfa" fill="#8b5cf6" fillOpacity={0.28} /><Tooltip formatter={(v) => [v, 'Средний score']} contentStyle={{ background: '#111b2d', border: '1px solid #2a3a55', borderRadius: 10 }} /></RadarChart></ResponsiveContainer></div>
      </section>
    </div>
  )
}

function Operations({ data, onSelectCall }) {
  const statuses = data?.status_distribution || []
  const durations = (data?.duration_distribution || []).map((x) => ({ ...x, label: DURATION_LABELS[x.bucket] || x.bucket }))
  const attention = data?.attention_calls || []
  const maxStatus = Math.max(1, ...statuses.map((x) => Number(x.value || 0)))
  return (
    <div className="operations-grid">
      <section className="panel">
        <div className="section-heading"><div><h2>Статусы пайплайна</h2><p>Текущее состояние обработанных звонков.</p></div></div>
        <div className="status-list">{statuses.map((item) => <div className="status-row" key={item.name}><div><span>{item.name}</span><strong>{item.value}</strong></div><div className="status-track"><i style={{ width: `${Number(item.value || 0) * 100 / maxStatus}%` }} /></div></div>)}</div>
      </section>
      <section className="panel">
        <div className="section-heading"><div><h2>Длительность</h2><p>Распределение разговоров по времени.</p></div></div>
        <div className="chart-box compact"><ResponsiveContainer width="100%" height="100%"><BarChart data={durations}><CartesianGrid strokeDasharray="3 3" stroke="#263349" /><XAxis dataKey="label" stroke="#8290a7" fontSize={10} /><YAxis stroke="#8290a7" fontSize={11} allowDecimals={false} /><Tooltip formatter={(v) => [v, 'Звонков']} contentStyle={{ background: '#111b2d', border: '1px solid #2a3a55', borderRadius: 10 }} /><Bar dataKey="value" fill="#22c55e" radius={[5, 5, 0, 0]} /></BarChart></ResponsiveContainer></div>
      </section>
      <section className="panel attention-panel">
        <div className="section-heading"><div><h2>Требуют внимания</h2><p>Ошибки, critical и звонки со score ниже 60.</p></div><Badge kind={attention.length ? 'critical' : 'normal'}>{attention.length}</Badge></div>
        <div className="attention-list">{attention.length === 0 && <div className="empty-state">Проблемных звонков в текущем срезе нет.</div>}{attention.map((call) => <button key={call.id} className="attention-row" onClick={() => onSelectCall(call.id)}><div><strong>{call.manager_name || '—'}</strong><span>{formatDateTime(call.started_at)} · {formatDuration(call.duration_seconds)}</span></div><div className="attention-tags"><Score value={call.score} /><RiskBadge risk={call.risk_level} />{call.error && <span className="error-dot" title={call.error}>!</span>}</div></button>)}</div>
      </section>
    </div>
  )
}

function Managers({ managers = [], onSelectManager }) {
  return (
    <section className="panel managers-panel">
      <div className="section-heading"><div><h2>Менеджеры</h2><p>Нажмите на сотрудника, чтобы отфильтровать весь дашборд.</p></div></div>

      <div className="table-wrap managers-table">
        <table><thead><tr><th>Менеджер</th><th>Звонки</th><th>Score</th><th>Coverage</th><th>Крит.</th><th>Сделки</th><th>Конверсия</th><th>Записи</th><th>Ср. длит.</th></tr></thead><tbody>
          {managers.length === 0 && <tr><td colSpan="9" className="empty-cell">Нет данных</td></tr>}
          {managers.map((m) => <tr key={m.name} className="click-row" onClick={() => onSelectManager(m.name)}><td className="primary-cell">{m.name}</td><td>{m.calls}</td><td><Score value={m.avg_score} /></td><td>{m.analysis_coverage ?? '—'}%</td><td>{m.critical} <span className="subline-inline">({m.critical_rate ?? '—'}%)</span></td><td>{m.completed_deals}</td><td>{m.deal_rate ?? '—'}%</td><td>{m.appointments}</td><td>{formatDuration(m.avg_duration_seconds)}</td></tr>)}
        </tbody></table>

      </div>
    </section>
  )
}

function SortHeader({ label, field, sort, onSort }) {
  const active = sort.by === field
  return <button className={`sort-header ${active ? 'active' : ''}`} onClick={() => onSort(field)}>{label}<span>{active ? (sort.order === 'asc' ? '↑' : '↓') : '↕'}</span></button>
}

function CallsTable({ data, loading, page, setPage, pageSize, setPageSize, sort, setSort, onSelect }) {
  const onSort = (field) => setSort((prev) => prev.by === field ? { by: field, order: prev.order === 'asc' ? 'desc' : 'asc' } : { by: field, order: 'desc' })
  const items = data?.items || []
  return (
    <section className="panel calls-panel">
      <div className="section-heading"><div><h2>Все звонки</h2><p>{data ? `${data.total.toLocaleString('ru-RU')} записей` : 'Загрузка…'} · строка открывает полную карточку.</p></div><div className="heading-actions">{loading && <span className="loading-pill">Обновление…</span>}<label className="page-size">На странице <select value={pageSize} onChange={(e) => { setPageSize(Number(e.target.value)); setPage(1) }}><option>25</option><option>50</option><option>100</option><option>200</option></select></label></div></div>
      <div className="table-wrap"><table className="calls-table"><thead><tr><th><SortHeader label="Время" field="started_at" sort={sort} onSort={onSort} /></th><th><SortHeader label="Менеджер" field="manager" sort={sort} onSort={onSort} /></th><th>Телефон</th><th>Напр.</th><th><SortHeader label="Длительность" field="duration_seconds" sort={sort} onSort={onSort} /></th><th><SortHeader label="Тип" field="call_type" sort={sort} onSort={onSort} /></th><th><SortHeader label="Score" field="score" sort={sort} onSort={onSort} /></th><th><SortHeader label="Риск" field="risk_level" sort={sort} onSort={onSort} /></th><th><SortHeader label="Статус" field="status" sort={sort} onSort={onSort} /></th><th>Доставка</th></tr></thead><tbody>
        {!loading && items.length === 0 && <tr><td colSpan="10" className="empty-cell">По выбранным фильтрам звонков нет</td></tr>}
        {items.map((call) => <tr key={call.id} onClick={() => onSelect(call.id)} className="click-row"><td><div className="primary-cell">{formatDateTime(call.started_at)}</div><div className="subline">{call.mango_account}</div></td><td><div className="primary-cell">{call.manager_name || '—'}</div><div className="subline">{call.manager_department || call.manager_position || ''}</div></td><td><div>{call.from_number || '—'}</div><div className="subline">→ {call.to_number || '—'}</div></td><td><Badge>{call.direction || '—'}</Badge></td><td>{formatDuration(call.duration_seconds)}</td><td>{call.call_type ? <Badge kind="type">{TYPE_LABELS[call.call_type] || call.call_type}</Badge> : <span className="muted">—</span>}</td><td><Score value={call.score} /></td><td><RiskBadge risk={call.risk_level} /></td><td><Badge>{call.status}</Badge>{call.error && <span className="error-dot" title={call.error}>!</span>}</td><td><span className={`delivery-dot ${call.has_notification ? 'ok' : ''}`} title={call.has_notification ? 'Уведомление отправлено' : 'Нет main notification'} /></td></tr>)}
      </tbody></table></div>
      <div className="pagination"><button className="btn btn-ghost" disabled={page <= 1} onClick={() => setPage((p) => Math.max(1, p - 1))}>← Назад</button><span>Страница <strong>{data?.page || page}</strong> из <strong>{data?.pages || 1}</strong></span><button className="btn btn-ghost" disabled={!data || page >= data.pages} onClick={() => setPage((p) => p + 1)}>Вперёд →</button></div>
    </section>
  )
}

function JsonList({ value }) {
  if (!value || (Array.isArray(value) && value.length === 0)) return <span className="muted">Нет</span>
  if (Array.isArray(value)) return <ul className="detail-list">{value.map((x, i) => <li key={i}>{typeof x === 'string' ? x : JSON.stringify(x)}</li>)}</ul>
  return <pre className="json-block">{JSON.stringify(value, null, 2)}</pre>
}

function CallDrawer({ callId, onClose, onUnauthorized }) {
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const [tab, setTab] = useState('overview')
  const [copied, setCopied] = useState(false)
  useEffect(() => {
    if (!callId) return
    const controller = new AbortController()
    setData(null); setError(''); setTab('overview'); setCopied(false)
    fetchJson(`${API}/analytics/calls/${encodeURIComponent(callId)}`, { signal: controller.signal }).then(setData).catch((e) => { if (e.name === 'AbortError') return; if (e.status === 401) onUnauthorized(); else setError(e.message) })
    return () => controller.abort()
  }, [callId, onUnauthorized])
  if (!callId) return null
  const criteria = data?.criteria || {}
  const analysis = data?.quality_raw?.analysis || {}
  const evidence = data?.quality_raw?.quality_control?.evidence_grounding || []
  const copyTranscript = async () => {
    if (!data?.transcript) return
    await navigator.clipboard.writeText(data.transcript)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }
  return (
    <div className="drawer-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose() }}><aside className="drawer">
      <div className="drawer-head"><div><div className="eyebrow">Карточка звонка</div><h2>{data?.manager_name || 'Загрузка…'}</h2><div className="subline selectable">{callId}</div></div><button className="close-btn" onClick={onClose}>×</button></div>
      <div className="drawer-tabs"><button className={tab === 'overview' ? 'active' : ''} onClick={() => setTab('overview')}>Обзор</button><button className={tab === 'transcript' ? 'active' : ''} onClick={() => setTab('transcript')}>Транскрипт</button><button className={tab === 'technical' ? 'active' : ''} onClick={() => setTab('technical')}>Техническое</button></div>
      {error && <div className="error-banner drawer-error">{error}</div>}
      {!data && !error && <div className="drawer-loading">Загрузка звонка…</div>}
      {data && <div className="drawer-body">
        {tab === 'overview' && <>
          <div className="detail-kpis"><div><span>Score</span><Score value={data.score} /></div><div><span>Риск</span><RiskBadge risk={data.risk_level} /></div><div><span>Тип</span><strong>{TYPE_LABELS[data.call_type] || data.call_type || '—'}</strong></div><div><span>Длительность</span><strong>{formatDuration(data.duration_seconds)}</strong></div></div>
          {data.audio_url && <div className="detail-section"><h3>Запись разговора</h3><audio controls preload="metadata" src={data.audio_url} /></div>}
          <div className="detail-grid"><div><span>Начало</span><strong>{formatDateTime(data.started_at)}</strong></div><div><span>АТС / группа</span><strong>{data.mango_account}</strong></div><div><span>Откуда</span><strong>{data.from_number || '—'}</strong></div><div><span>Куда</span><strong>{data.to_number || '—'}</strong></div><div><span>Добавочный</span><strong>{data.manager_extension || '—'}</strong></div><div><span>Статус</span><strong>{data.status}</strong></div></div>
          {data.error && <div className="error-banner"><strong>Ошибка пайплайна:</strong> {data.error}</div>}
          <div className="detail-section"><h3>Итог анализа</h3><p>{data.summary || 'Анализ отсутствует.'}</p></div>
          {data.risk_reason && <div className="detail-section"><h3>Причина риска</h3><p>{data.risk_reason}</p></div>}
          {data.recommendation && <div className="detail-section accent-section"><h3>Рекомендация</h3><p>{data.recommendation}</p></div>}
          <div className="detail-section"><h3>Критерии качества</h3><div className="criteria-bars">{Object.entries(CRITERIA_LABELS).map(([key, label]) => <div key={key}><div className="criteria-row"><span>{label}</span><strong>{criteria[key] ?? '—'}</strong></div><div className="progress"><i style={{ width: `${criteria[key] ?? 0}%` }} /></div></div>)}</div></div>
          <div className="detail-section"><h3>Ошибки / замечания</h3><JsonList value={data.errors} /></div>
          {data.critical_errors?.length > 0 && <div className="detail-section danger-section"><h3>Критические ошибки</h3><JsonList value={data.critical_errors} /></div>}
          {analysis.next_step && <div className="detail-section"><h3>Следующий шаг</h3><p><strong>{analysis.next_step.status}</strong>{analysis.next_step.quote ? ` · ${analysis.next_step.quote}` : ''}</p></div>}
          {analysis.objections?.length > 0 && <div className="detail-section"><h3>Возражения</h3><JsonList value={analysis.objections} /></div>}
        </>}
        {tab === 'transcript' && <div className="detail-section transcript-section"><div className="detail-section-head"><h3>Полный транскрипт</h3><button className="btn btn-ghost btn-small" onClick={copyTranscript}>{copied ? 'Скопировано' : 'Копировать'}</button></div><pre className="transcript transcript-full">{data.transcript || 'Транскрипция отсутствует.'}</pre></div>}
        {tab === 'technical' && <>
          <div className="detail-grid"><div><span>Call ID</span><strong className="selectable">{data.id}</strong></div><div><span>Recording ID</span><strong className="selectable">{data.recording_id || '—'}</strong></div><div><span>Уведомления</span><strong>{data.notification_count ?? 0}</strong></div><div><span>STT model</span><strong>{data.transcription_model || '—'}</strong></div><div><span>Quality model</span><strong>{data.quality_model || '—'}</strong></div><div><span>Class model</span><strong>{data.classification_model || '—'}</strong></div></div>
          {data.classification_reason && <div className="detail-section"><h3>Причина классификации</h3><p>{data.classification_reason}</p></div>}
          {analysis.criteria_evidence && <div className="detail-section"><h3>Evidence по критериям</h3><JsonList value={analysis.criteria_evidence} /></div>}
          {evidence.length > 0 && <div className="detail-section"><h3>Grounding evidence</h3><JsonList value={evidence} /></div>}
          <div className="detail-section"><h3>Raw Mango metadata</h3><JsonList value={data.raw} /></div>
        </>}
      </div>}
    </aside></div>
  )
}

function DashboardApp({ user, onLogout, onUnauthorized }) {
  const [filters, setFilters] = useState(initialFilters)
  const [debouncedFilters, setDebouncedFilters] = useState(filters)
  const [options, setOptions] = useState({})
  const [overview, setOverview] = useState(null)
  const [calls, setCalls] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(50)
  const [sort, setSort] = useState({ by: 'started_at', order: 'desc' })
  const [selectedCall, setSelectedCall] = useState(null)

  const handleError = (err) => {
    if (err.name === 'AbortError') return
    if (err.status === 401) { onUnauthorized(); return }
    setError(err.message)
  }

  useEffect(() => {
    fetchJson(`${API}/analytics/filters`).then(setOptions).catch(handleError)
  }, [])
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedFilters(filters), 350)
    return () => clearTimeout(timer)
  }, [filters])
  useEffect(() => { setPage(1) }, [debouncedFilters])

  const filterQuery = useMemo(() => buildQuery(debouncedFilters), [debouncedFilters])
  const exportHref = `${API}/analytics/export.csv?${filterQuery}`
  useEffect(() => {
    const controller = new AbortController()
    setLoading(true); setError('')
    Promise.all([
      fetchJson(`${API}/analytics/overview?${filterQuery}`, { signal: controller.signal }),
      fetchJson(`${API}/analytics/calls?${buildQuery(debouncedFilters, { page, page_size: pageSize, sort_by: sort.by, sort_order: sort.order })}`, { signal: controller.signal }),
    ]).then(([overviewData, callsData]) => { setOverview(overviewData); setCalls(callsData) }).catch(handleError).finally(() => setLoading(false))
    return () => controller.abort()
  }, [filterQuery, debouncedFilters, page, pageSize, sort])

  return (
    <div className="app-shell">
      <header className="topbar"><div><div className="brand-row"><span className="brand-mark">D</span><span className="brand">DALEON</span><Badge kind="live">LIVE ANALYTICS</Badge></div><h1>Аналитика звонков</h1><p>Mango → транскрипция → классификация → оценка качества → Telegram.</p></div><div className="topbar-right"><div className="topbar-meta"><span className="status-dot" />PostgreSQL online</div><div className="user-menu"><span className="user-avatar">{(user?.username || 'A')[0].toUpperCase()}</span><div><strong>{user?.username || 'admin'}</strong><span>Администратор</span></div><button className="btn btn-ghost btn-small" onClick={onLogout}>Выйти</button></div></div></header>
      <main>
        {error && <div className="error-banner global-error"><strong>Ошибка загрузки:</strong> {error}</div>}
        <Filters filters={filters} setFilters={setFilters} options={options} onReset={() => setFilters(initialFilters())} exportHref={exportHref} />
        <Summary data={overview} />
        <PipelinePanel summary={overview?.summary || {}} />
        <Charts data={overview} />
        <Operations data={overview} onSelectCall={setSelectedCall} />
        <Managers managers={overview?.managers || []} onSelectManager={(manager) => setFilters((prev) => ({ ...prev, manager }))} />
        <CallsTable data={calls} loading={loading} page={page} setPage={setPage} pageSize={pageSize} setPageSize={setPageSize} sort={sort} setSort={setSort} onSelect={setSelectedCall} />
      </main>
      <CallDrawer callId={selectedCall} onClose={() => setSelectedCall(null)} onUnauthorized={onUnauthorized} />
    </div>
  )
}

function Root() {
  const [auth, setAuth] = useState({ loading: true, user: null })
  useEffect(() => {
    fetchJson(`${API}/auth/me`).then((user) => setAuth({ loading: false, user })).catch((err) => {
      if (err.status === 401) setAuth({ loading: false, user: null })
      else setAuth({ loading: false, user: null, error: err.message })
    })
  }, [])

  const logout = async () => {
    try { await fetchJson(`${API}/auth/logout`, { method: 'POST' }) } catch (_) {}
    setAuth({ loading: false, user: null })
  }

  if (auth.loading) return <div className="boot-screen"><span className="brand-mark">D</span><div>Проверка сессии…</div></div>
  if (!auth.user) return <LoginScreen onAuthenticated={(user) => setAuth({ loading: false, user })} />
  return <DashboardApp user={auth.user} onLogout={logout} onUnauthorized={() => setAuth({ loading: false, user: null })} />
}

createRoot(document.getElementById('root')).render(<React.StrictMode><Root /></React.StrictMode>)

