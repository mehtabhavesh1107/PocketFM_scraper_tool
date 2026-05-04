import { useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import './App.css';

// Default to a same-origin relative path so any host that serves the UI (LAN IP, tunnel,
// deployed domain) can reach the API through the dev-server / reverse proxy. Override with
// VITE_API_BASE_URL only when the API lives on a separate origin.
const API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) || '/api').replace(/\/$/, '');
const WORKSPACE_KEY = 'pocketfm_workspace_id';

type PageId = 'dashboard' | 'scrapping' | 'mapping' | 'benchmark' | 'outreach' | 'export';
type SourceType = 'amazon' | 'goodreads' | 'shared';
type JobKind = 'scrape' | 'scrape-fast' | 'enrich-goodreads' | 'enrich-contacts';

interface Batch {
  id: number;
  workspace_id: string;
  name: string;
  genre: string;
  subgenre: string;
  description: string;
  status: string;
  created_at: string;
  updated_at: string;
}

interface BootstrapResponse {
  batch: Batch;
  runs: Batch[];
  summary: BatchSummary;
  active_job?: Job | null;
}

interface BatchSummary {
  batch_id: number;
  name: string;
  total_sources: number;
  total_books: number;
  shortlisted_books: number;
  outreach_ready: number;
  emails_found: number;
  job_counts: Record<string, number>;
}

interface Contact {
  email_id?: string;
  email_source_note?: string;
  email_type?: string;
  contact_forms?: string;
  facebook_link?: string;
  publisher_details?: string;
  website?: string;
  author_email?: string;
  agent_email?: string;
}

interface Evaluation {
  story_score?: number | null;
  characters_score?: number | null;
  hooks_score?: number | null;
  series_potential_score?: number | null;
  audio_adaptability_score?: number | null;
  india_fit_score?: number | null;
  notes?: string;
}

interface OutreachMessage {
  id: number;
  recipient?: string;
  cc?: string;
  subject?: string;
  body?: string;
  template?: string;
  status?: string;
  sent_at?: string | null;
}

interface Book {
  id: number;
  batch_id: number;
  title: string;
  author: string;
  url?: string;
  amazon_url?: string;
  rating?: number | null;
  rating_count?: number | null;
  publisher?: string;
  publication_date?: string;
  part_of_series?: string;
  language?: string;
  best_sellers_rank?: string;
  print_length?: string;
  book_number?: string;
  format?: string;
  synopsis?: string;
  genre?: string;
  sub_genre?: string;
  cleaned_series_name?: string;
  series_flag?: string;
  total_pages_in_series?: string;
  total_word_count?: string;
  total_hours?: string;
  goodread_link?: string;
  series_book_1?: string;
  series_link?: string;
  remarks?: string;
  primary_book_count?: string;
  goodreads_rating?: string;
  goodreads_rating_count?: string;
  word_count?: number | null;
  audio_score?: number | null;
  book_type?: string;
  benchmark_match?: boolean;
  shortlisted?: boolean;
  provenance_json?: Record<string, unknown>;
  contact?: Contact | null;
  evaluation?: Evaluation | null;
  outreach_messages?: OutreachMessage[];
}

interface BooksPage {
  total: number;
  items: Book[];
}

interface Job {
  id: string;
  batch_id: number;
  stage: string;
  status: string;
  message: string;
  progress_current: number;
  progress_total: number;
  progress_percent: number;
  error?: string;
}

function isRunningJob(job?: Job | null): boolean {
  return job?.status === 'queued' || job?.status === 'running';
}

function jobStageLabel(job: Job): string {
  if (job.stage === 'fast_scrape') return 'fast scrape';
  if (job.stage === 'enrich_goodreads') return 'enrich Goodreads';
  if (job.stage === 'enrich_contacts') return 'find contacts';
  return job.stage.replace(/_/g, ' ');
}

interface FieldDefinition {
  name: string;
  label: string;
  type: string;
  required: boolean;
  on: boolean;
}

interface StoredSchema {
  id: number;
  batch_id?: number | null;
  source_type: string;
  name: string;
  file_name: string;
  file_format: string;
  fields_json: FieldDefinition[];
  selected_fields_json: string[];
  created_at: string;
}

interface SourceRow {
  source_type: 'amazon' | 'goodreads';
  url: string;
  max_results: number;
  output_format: string;
}

interface SourceInput {
  url: string;
  output_format: string;
}

interface ExportRecord {
  id: number;
  export_format: string;
  file_path: string;
  row_count: number;
  metadata_json?: Record<string, unknown>;
}

interface DataQualityIssue {
  code: string;
  severity: 'critical' | 'warning';
  message: string;
  field?: string;
}

interface DataQualityRow {
  book_id: number;
  title: string;
  author: string;
  quality_score: number;
  critical_count: number;
  warning_count: number;
  issues: DataQualityIssue[];
  missing_fields: string[];
  genre: string;
  sub_genre: string;
  genre_source: string;
  genre_reason: string;
  source_asin?: string;
  detail_asin?: string;
  detail_url?: string;
  goodreads_link?: string;
  goodreads_resolved_link?: string;
  goodreads_match_status?: string;
  goodreads_match_confidence?: number;
  goodreads_match_reason?: string;
  goodreads_match_method?: string;
  goodreads_candidates?: GoodreadsCandidate[];
  goodreads_isbns_used?: string[];
  contact_ready: boolean;
}

interface GoodreadsCandidate {
  url: string;
  title?: string;
  author?: string;
  series_name?: string;
  series_url?: string;
  rating?: string;
  rating_count?: string;
  pages?: string;
  published_year?: string;
  publication?: string;
  publisher?: string;
  isbn_10?: string;
  isbn_13?: string;
  score?: number;
  match_method?: string;
  evidence?: string[];
}

interface DataQualityReport {
  total: number;
  ready: boolean;
  critical_count: number;
  warning_count: number;
  coverage: Record<string, number>;
  missing: Record<string, number>;
  issue_counts: Record<string, number>;
  genre_sources: Record<string, number>;
  goodreads?: {
    status_counts: Record<string, number>;
    review_candidate_count: number;
    average_confidence: number;
  };
  rows: DataQualityRow[];
}

interface Filters {
  min_rating: number;
  min_reviews: number;
  min_word_count: number;
  max_series_books: number;
  min_audio_score: number;
}

const pageLabels: Record<PageId, string> = {
  dashboard: 'Dashboard',
  scrapping: 'Scraping',
  mapping: 'Data & Genre Mapping',
  benchmark: 'Benchmark Filters',
  outreach: 'Author Outreach',
  export: 'Export & Share',
};

const initialSourceInputs: Record<'amazon' | 'goodreads', SourceInput[]> = {
  amazon: [
    { url: '', output_format: 'CSV' },
    { url: '', output_format: 'CSV' },
    { url: '', output_format: 'CSV' },
    { url: '', output_format: 'CSV' },
  ],
  goodreads: [
    { url: '', output_format: 'CSV' },
    { url: '', output_format: 'CSV' },
    { url: '', output_format: 'CSV' },
    { url: '', output_format: 'CSV' },
  ],
};

function createWorkspaceId(): string {
  const random = typeof crypto !== 'undefined' && crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `ws-${random}`;
}

function getWorkspaceId(): string {
  if (typeof window === 'undefined') return 'public';
  const existing = window.localStorage.getItem(WORKSPACE_KEY);
  if (existing) return existing;
  const created = createWorkspaceId();
  window.localStorage.setItem(WORKSPACE_KEY, created);
  return created;
}

function emptySourceInputs(): Record<'amazon' | 'goodreads', SourceInput[]> {
  return {
    amazon: initialSourceInputs.amazon.map((row) => ({ ...row })),
    goodreads: initialSourceInputs.goodreads.map((row) => ({ ...row })),
  };
}

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('X-Workspace-Id', getWorkspaceId());
  if (init.body && !(init.body instanceof FormData) && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }
  const response = await fetch(`${API_BASE}${path}`, { ...init, headers });
  if (!response.ok) {
    const raw = await response.text();
    let message = raw;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        if (Array.isArray(parsed.detail)) {
          message = parsed.detail
            .map((entry: { loc?: (string | number)[]; msg?: string }) => {
              const field = entry.loc ? entry.loc.filter((p) => p !== 'body').join('.') : '';
              return field ? `${field}: ${entry.msg}` : entry.msg;
            })
            .join('; ');
        } else if (typeof parsed.detail === 'string') {
          message = parsed.detail;
        } else if (typeof parsed.message === 'string') {
          message = parsed.message;
        }
      }
    } catch {
      /* response was not JSON; use raw text */
    }
    throw new Error(message || `${response.status} ${response.statusText}`);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

function fmtNumber(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1000) return `${Math.round(value / 1000)}K`;
  return value.toLocaleString();
}

function sourceFor(book: Book): string {
  const provenance = book.provenance_json || {};
  if (book.amazon_url || provenance.amazon) return 'Amazon';
  if (book.goodread_link || provenance.goodreads) return 'Goodreads';
  return String(provenance.source || 'Manual');
}

function contactEmail(book: Book): string {
  return book.contact?.email_id || book.contact?.author_email || book.contact?.agent_email || '';
}

function seriesCount(book: Book): number {
  const parsed = Number.parseInt(book.primary_book_count || book.book_number || '1', 10);
  return Number.isFinite(parsed) ? parsed : 1;
}

function wordCount(book: Book): number {
  const fromString = Number.parseInt((book.total_word_count || '').replace(/,/g, ''), 10);
  return book.word_count || (Number.isFinite(fromString) ? fromString : 0);
}

function audioScore(book: Book): number {
  return book.audio_score || 0;
}

function genreTagClass(genre = ''): string {
  const map: Record<string, string> = {
    Romance: 'tg-c',
    Fantasy: 'tg-p',
    'Sci-Fi': 'tg-t',
    Thriller: 'tg-a',
    Mystery: 'tg-g',
    Historical: 'tg-gr',
    Contemporary: 'tg-b',
    Satire: 'tg-g',
  };
  return map[genre] || 'tg-g';
}

function stars(value?: number | null): string {
  if (!value) return '-';
  return `${value.toFixed(1)} star`;
}

function App() {
  const [workspaceId] = useState(() => getWorkspaceId());
  const [activePage, setActivePage] = useState<PageId>('dashboard');
  const [batch, setBatch] = useState<Batch | null>(null);
  const [runs, setRuns] = useState<Batch[]>([]);
  const [summary, setSummary] = useState<BatchSummary | null>(null);
  const [books, setBooks] = useState<Book[]>([]);
  const [dataQuality, setDataQuality] = useState<DataQualityReport | null>(null);
  const [schemas, setSchemas] = useState<Partial<Record<SourceType, StoredSchema>>>({});
  const [schemaTab, setSchemaTab] = useState<SourceType>('amazon');
  const [sourceInputs, setSourceInputs] = useState(emptySourceInputs);
  const [activeJob, setActiveJob] = useState<Job | null>(null);
  const [notice, setNotice] = useState('');
  const [error, setError] = useState('');
  const [search, setSearch] = useState('');
  const [genreFilter, setGenreFilter] = useState('');
  const [sortKey, setSortKey] = useState<'title' | 'author' | 'rating' | 'rating_count' | 'word_count'>('title');
  const [filters, setFilters] = useState<Filters>({
    min_rating: 3.8,
    min_reviews: 5000,
    min_word_count: 50000,
    max_series_books: 10,
    min_audio_score: 60,
  });
  const [activeGenres, setActiveGenres] = useState<string[]>([]);
  const [activeTypes, setActiveTypes] = useState<string[]>([]);
  const [selectedEvalId, setSelectedEvalId] = useState<number | null>(null);
  const [selectedOutreachId, setSelectedOutreachId] = useState<number | null>(null);
  const [emailDraft, setEmailDraft] = useState({ recipient: '', cc: 'astha.singh@pocketfm.com', subject: '', body: '' });
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    bootstrap();
  }, []);

  useEffect(() => {
    if (!batch) return;
    const handle = window.setTimeout(() => {
      applyBenchmark(false);
    }, 350);
    return () => window.clearTimeout(handle);
  }, [filters, activeGenres, activeTypes, batch?.id]);

  const shortlisted = useMemo(() => books.filter((book) => book.shortlisted), [books]);
  const selectedEvalBook = useMemo(
    () => books.find((book) => book.id === selectedEvalId) || shortlisted[0] || books[0],
    [books, shortlisted, selectedEvalId],
  );
  const outreachBooks = shortlisted.length ? shortlisted : books;
  const selectedOutreachBook = useMemo(
    () => outreachBooks.find((book) => book.id === selectedOutreachId) || outreachBooks[0],
    [outreachBooks, selectedOutreachId],
  );

  useEffect(() => {
    if (!selectedOutreachBook) return;
    const message = selectedOutreachBook.outreach_messages?.[0];
    setEmailDraft({
      recipient: message?.recipient || contactEmail(selectedOutreachBook),
      cc: message?.cc || 'astha.singh@pocketfm.com',
      subject: message?.subject || `Commissioning inquiry - ${selectedOutreachBook.title} · Pocket FM`,
      body: message?.body || buildLocalTemplate(selectedOutreachBook, 'formal'),
    });
  }, [selectedOutreachBook?.id, selectedOutreachBook?.outreach_messages?.[0]?.id]);

  async function bootstrap() {
    setError('');
    try {
      const data = await api<BootstrapResponse>('/bootstrap', { method: 'POST' });
      setBatch(data.batch);
      setRuns(data.runs || [data.batch]);
      setSummary(data.summary);
      setActiveJob(data.active_job || null);
      await Promise.all([loadBooks(data.batch.id, true), loadSources(data.batch.id), loadReferenceSchema(data.batch.id), loadDataQuality(data.batch.id)]);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not connect to backend');
    }
  }

  async function loadRuns() {
    const data = await api<Batch[]>('/batches');
    setRuns(data);
    return data;
  }

  async function loadBatch(batchId: number, nextRuns?: Batch[]) {
    setError('');
    setNotice('');
    const [nextBatch, nextSummary] = await Promise.all([
      api<Batch>(`/batches/${batchId}`),
      api<BatchSummary>(`/batches/${batchId}/summary`),
    ]);
    setBatch(nextBatch);
    setSummary(nextSummary);
    setActiveJob(null);
    setBooks([]);
    setDataQuality(null);
    setSourceInputs(emptySourceInputs());
    setSelectedEvalId(null);
    setSelectedOutreachId(null);
    if (nextRuns) setRuns(nextRuns);
    await Promise.all([loadBooks(nextBatch.id, true), loadSources(nextBatch.id), loadReferenceSchema(nextBatch.id), loadDataQuality(nextBatch.id)]);
  }

  async function selectRun(batchId: number) {
    try {
      await loadBatch(batchId);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not load run');
    }
  }

  async function createNewRun() {
    try {
      const created = await api<Batch>('/batches', {
        method: 'POST',
        body: JSON.stringify({
          name: `Run ${new Date().toLocaleString()}`,
          genre: '',
          subgenre: '',
          description: '',
        }),
      });
      const nextRuns = await loadRuns();
      await loadBatch(created.id, nextRuns);
      setNotice('New run ready. Paste Amazon or Goodreads links to start.');
      setActivePage('scrapping');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not create run');
    }
  }

  async function loadSummary(batchId = batch?.id) {
    if (!batchId) return;
    const data = await api<BatchSummary>(`/batches/${batchId}/summary`);
    setSummary(data);
  }

  async function loadBooks(batchId = batch?.id, resetSelection = false) {
    if (!batchId) return;
    const page = await api<BooksPage>(`/batches/${batchId}/books?page_size=500`);
    setBooks(page.items);
    if (resetSelection) {
      setSelectedEvalId(page.items[0]?.id || null);
      setSelectedOutreachId(page.items[0]?.id || null);
    } else {
      if (!selectedEvalId && page.items[0]) setSelectedEvalId(page.items[0].id);
      if (!selectedOutreachId && page.items[0]) setSelectedOutreachId(page.items[0].id);
    }
  }

  async function loadDataQuality(batchId = batch?.id) {
    if (!batchId) return;
    const report = await api<DataQualityReport>(`/batches/${batchId}/data-quality`);
    setDataQuality(report);
  }

  async function loadSources(batchId = batch?.id) {
    if (!batchId) return;
    const sources = await api<SourceRow[]>(`/batches/${batchId}/sources`);
    const next = emptySourceInputs();
    if (!sources.length) {
      setSourceInputs(next);
      return;
    }
    (['amazon', 'goodreads'] as const).forEach((source) => {
      sources
        .filter((item) => item.source_type === source)
        .slice(0, 4)
        .forEach((item, index) => {
          next[source][index] = {
            url: item.url,
            output_format: item.output_format,
          };
        });
    });
    setSourceInputs(next);
  }

  async function loadReferenceSchema(batchId = batch?.id) {
    const data = await api<{ fields: FieldDefinition[] }>('/reference-schema');
    const selected = data.fields.map((field) => field.name);
    const referenceSchema: StoredSchema = {
      id: 0,
      batch_id: batchId || null,
      source_type: 'reference',
      name: 'Local reference sheet',
      file_name: 'contact_live_sheet_snapshot.csv',
      file_format: 'csv',
      fields_json: data.fields,
      selected_fields_json: selected,
      created_at: new Date().toISOString(),
    };
    setSchemas({ amazon: referenceSchema, goodreads: referenceSchema, shared: referenceSchema });
  }

  async function refreshAll() {
    if (!batch) return;
    await Promise.all([loadBooks(batch.id), loadSummary(batch.id), loadSources(batch.id), loadDataQuality(batch.id), loadRuns()]);
  }

  function nav(page: PageId) {
    setActivePage(page);
  }

  function updateSource(source: 'amazon' | 'goodreads', index: number, patch: Partial<SourceInput>) {
    setSourceInputs((prev) => ({
      ...prev,
      [source]: prev[source].map((row, rowIndex) => (rowIndex === index ? { ...row, ...patch } : row)),
    }));
  }

  async function saveSources() {
    if (!batch) return [];
    const payload: SourceRow[] = [];
    (['amazon', 'goodreads'] as const).forEach((source) => {
      sourceInputs[source].forEach((row) => {
        if (row.url.trim()) {
          payload.push({
            source_type: source,
            url: row.url.trim(),
            max_results: 0,
            output_format: row.output_format,
          });
        }
      });
    });
    if (!payload.length) {
      throw new Error('Add at least one Amazon or Goodreads URL before running a scrape.');
    }
    return api<SourceRow[]>(`/batches/${batch.id}/sources`, {
      method: 'PUT',
      body: JSON.stringify(payload),
    });
  }

  async function runJob(kind: JobKind) {
    if (!batch) return;
    if (activeJob && isRunningJob(activeJob)) {
      setNotice(`${jobStageLabel(activeJob)} is already running.`);
      return;
    }
    setError('');
    setNotice('');
    try {
      if (kind === 'scrape' || kind === 'scrape-fast') await saveSources();
      const data = await api<{ job: Job }>(`/batches/${batch.id}/jobs/${kind}`, { method: 'POST' });
      setActiveJob(data.job);
      setNotice(`${data.job.stage.replace(/_/g, ' ')} queued.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not start job');
    }
  }

  useEffect(() => {
    if (!isRunningJob(activeJob)) return;
    let cancelled = false;
    pollJob(activeJob.id, () => cancelled);
    return () => {
      cancelled = true;
    };
  }, [activeJob?.id]);

  async function pollJob(jobId: string, shouldCancel: () => boolean = () => false) {
    let consecutiveErrors = 0;
    for (let attempt = 0; attempt < 1800; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 1200));
      if (shouldCancel()) return;
      try {
        const job = await api<Job>(`/jobs/${jobId}`);
        consecutiveErrors = 0;
        setActiveJob(job);
        if (job.status === 'completed' || job.status === 'failed') {
          if (job.status === 'failed') {
            setError(job.error || job.message || 'Job failed.');
            setNotice('');
          } else {
            setNotice(job.message);
            setError('');
          }
          await refreshAll();
          return;
        }
      } catch (err) {
        consecutiveErrors += 1;
        if (consecutiveErrors >= 5) {
          setError(`Lost contact with backend while running job: ${err instanceof Error ? err.message : err}`);
          return;
        }
      }
    }
  }

  async function toggleSchemaField(source: SourceType, field: FieldDefinition) {
    const schema = schemas[source];
    if (!schema || field.required) return;
    const selected = new Set(schema.selected_fields_json);
    if (selected.has(field.name)) selected.delete(field.name);
    else selected.add(field.name);
    const updated = await api<StoredSchema>(`/schemas/${schema.id}`, {
      method: 'PATCH',
      body: JSON.stringify({ selected_fields: Array.from(selected) }),
    });
    setSchemas((prev) => ({ ...prev, [source]: updated }));
  }

  async function patchBook(bookId: number, patch: Partial<Book>) {
    const updated = await api<Book>(`/books/${bookId}`, { method: 'PATCH', body: JSON.stringify(patch) });
    setBooks((prev) => prev.map((book) => (book.id === bookId ? updated : book)));
  }

  async function acceptGoodreadsCandidate(bookId: number, candidate: GoodreadsCandidate) {
    try {
      const updated = await api<Book>(`/books/${bookId}/goodreads/accept`, {
        method: 'POST',
        body: JSON.stringify(candidate),
      });
      setBooks((prev) => prev.map((book) => (book.id === bookId ? updated : book)));
      if (batch) await loadDataQuality(batch.id);
      setNotice('Goodreads candidate accepted and row metrics refreshed.');
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Could not accept Goodreads candidate');
    }
  }

  async function applyBenchmark(showNotice = true) {
    if (!batch) return;
    try {
      const payload = {
        ...filters,
        genres: activeGenres,
        types: activeTypes,
      };
      const result = await api<{ total: number; matched_ids: number[] }>(`/batches/${batch.id}/benchmark/apply`, {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      await Promise.all([loadBooks(batch.id), loadSummary(batch.id)]);
      if (showNotice) setNotice(`${result.total} books matched the current benchmark.`);
    } catch (err) {
      if (showNotice) setError(err instanceof Error ? err.message : 'Could not apply filters');
    }
  }

  async function saveEvaluation(book: Book, patch: Partial<Evaluation>) {
    const updated = await api<Book>(`/books/${book.id}/evaluation`, { method: 'PATCH', body: JSON.stringify(patch) });
    setBooks((prev) => prev.map((item) => (item.id === book.id ? updated : item)));
  }

  async function createDraft(template: string) {
    if (!selectedOutreachBook) return;
    const updated = await api<Book>(`/books/${selectedOutreachBook.id}/outreach/draft`, {
      method: 'POST',
      body: JSON.stringify({ template, sender_name: 'Astha Singh', sender_email: 'astha.singh@pocketfm.com' }),
    });
    setBooks((prev) => prev.map((book) => (book.id === updated.id ? updated : book)));
  }

  async function saveOutreach(status = 'draft') {
    if (!selectedOutreachBook) return;
    setSaving(true);
    try {
      const updated = await api<Book>(`/books/${selectedOutreachBook.id}/outreach`, {
        method: 'PATCH',
        body: JSON.stringify({ ...emailDraft, status }),
      });
      setBooks((prev) => prev.map((book) => (book.id === updated.id ? updated : book)));
      setNotice(status === 'sent' ? 'Outreach marked as sent.' : 'Draft saved.');
    } finally {
      setSaving(false);
    }
  }

  async function sendOutreach() {
    if (!emailDraft.recipient.trim()) {
      setError('Add a recipient before sending outreach.');
      return;
    }
    const params = new URLSearchParams();
    if (emailDraft.cc.trim()) params.set('cc', emailDraft.cc.trim());
    params.set('subject', emailDraft.subject);
    params.set('body', emailDraft.body);
    window.location.href = `mailto:${encodeURIComponent(emailDraft.recipient.trim())}?${params.toString()}`;
    await saveOutreach('sent');
  }

  async function createExport(format: 'csv' | 'xlsx' | 'pdf' | 'json', profile = 'sample_compatible') {
    if (!batch) return;
    const record = await api<ExportRecord>(`/batches/${batch.id}/exports`, {
      method: 'POST',
      body: JSON.stringify({ export_format: format, profile }),
    });
    const downloadUrl = `${API_BASE}/exports/${record.id}/download?workspace_id=${encodeURIComponent(workspaceId)}`;
    window.open(downloadUrl, '_blank');
    await loadDataQuality(batch.id);
    setNotice(`${format.toUpperCase()} export created with ${record.row_count} rows.`);
  }

  const metrics = useMemo(() => {
    const total = books.length;
    const shortlistedBooks = shortlisted.length ? shortlisted : books;
    const denom = shortlistedBooks.length || 1;
    const avgRating = shortlistedBooks.reduce((sum, book) => sum + (book.rating || 0), 0) / denom;
    const avgWord = shortlistedBooks.reduce((sum, book) => sum + wordCount(book), 0) / denom;
    const avgAudio = shortlistedBooks.reduce((sum, book) => sum + audioScore(book), 0) / denom;
    return {
      total,
      avgRating,
      avgWord,
      avgAudio,
      emails: books.filter((book) => contactEmail(book)).length,
      contacted: books.filter((book) => book.outreach_messages?.some((message) => message.status === 'sent')).length,
    };
  }, [books, shortlisted]);

  const genres = useMemo(() => Array.from(new Set(books.map((book) => book.genre || '').filter(Boolean))).sort(), [books]);
  const jobRunning = isRunningJob(activeJob);
  const visibleMappingBooks = useMemo(() => {
    const term = search.trim().toLowerCase();
    return [...books]
      .filter((book) => {
        const matchesTerm =
          !term ||
          book.title.toLowerCase().includes(term) ||
          (book.author || '').toLowerCase().includes(term) ||
          (book.genre || '').toLowerCase().includes(term);
        const matchesGenre = !genreFilter || book.genre === genreFilter;
        return matchesTerm && matchesGenre;
      })
      .sort((left, right) => {
        if (sortKey === 'title' || sortKey === 'author') return String(left[sortKey] || '').localeCompare(String(right[sortKey] || ''));
        if (sortKey === 'word_count') return wordCount(right) - wordCount(left);
        return Number(right[sortKey] || 0) - Number(left[sortKey] || 0);
      });
  }, [books, search, genreFilter, sortKey]);

  return (
    <div className="commissioning-app">
      <header className="header">
        <div className="brand">
          <div className="brand-icon">P</div>
          <span className="brand-name">Commissioning Tool</span>
          <span className="brand-sep">/</span>
          <span className="brand-sub">Content Acquisition Pipeline</span>
        </div>
        <div className="header-right">
          <span className="pill-badge"><span className="live-dot" /> Live API</span>
          <span className="user-info">Astha Singh</span>
          <div className="avatar">AS</div>
        </div>
      </header>

      <div className="app-body">
        <aside className="sidebar">
          <div className="sidebar-section">
            <div className="sidebar-label">Overview</div>
            <NavItem page="dashboard" activePage={activePage} onClick={nav} icon="□" label="Dashboard" />
          </div>
          <div className="sidebar-section">
            <div className="sidebar-label">Pipeline</div>
            <NavItem page="scrapping" activePage={activePage} onClick={nav} icon="↗" label="Scraping" badge={summary?.total_sources || 0} />
            <NavItem page="mapping" activePage={activePage} onClick={nav} icon="▦" label="Data & Genre Mapping" badge={summary?.total_books || 0} />
            <NavItem page="benchmark" activePage={activePage} onClick={nav} icon="◎" label="Benchmark Filters" badge={summary?.shortlisted_books || 0} green />
          </div>
          <div className="sidebar-section">
            <div className="sidebar-label">Action</div>
            <NavItem page="outreach" activePage={activePage} onClick={nav} icon="✉" label="Author Outreach" />
            <NavItem page="export" activePage={activePage} onClick={nav} icon="⇩" label="Export & Share" />
          </div>
          <div className="sidebar-footer">
            <div className="progress-ring">
              <span>Pipeline progress</span>
              <span className="progress-percent">{Math.round(((summary?.shortlisted_books || 0) / Math.max(summary?.total_books || 1, 1)) * 100)}%</span>
            </div>
            <div className="ring-track">
              <div className="ring-fill" style={{ width: `${Math.round(((summary?.shortlisted_books || 0) / Math.max(summary?.total_books || 1, 1)) * 100)}%` }} />
            </div>
            <div className="sidebar-footnote">
              Workspace: {workspaceId.slice(-8)}
              <br />
              Run: {batch?.name || 'Connecting...'}
            </div>
          </div>
        </aside>

        <main className="main">
          {(error || notice || activeJob) && (
            <div className="top-alerts">
              {error && <div className="notif error show"><span>!</span>{error}<button className="btn btn-xs" onClick={() => setError('')}>Dismiss</button></div>}
              {notice && <div className="notif show"><span>✓</span>{notice}<button className="btn btn-xs" onClick={() => setNotice('')}>Dismiss</button></div>}
              {jobRunning && activeJob && <JobProgressCard job={activeJob} />}
            </div>
          )}

          {activePage === 'dashboard' && (
            <DashboardPage
              books={books}
              batch={batch}
              runs={runs}
              workspaceId={workspaceId}
              summary={summary}
              metrics={metrics}
              genres={genres}
              dataQuality={dataQuality}
              acceptGoodreadsCandidate={acceptGoodreadsCandidate}
              selectRun={selectRun}
              createNewRun={createNewRun}
              nav={nav}
            />
          )}
          {activePage === 'scrapping' && (
            <ScrapingPage
              schemaTab={schemaTab}
              setSchemaTab={setSchemaTab}
              schemas={schemas}
              sourceInputs={sourceInputs}
              updateSource={updateSource}
              toggleSchemaField={toggleSchemaField}
              runJob={runJob}
              jobRunning={jobRunning}
              nav={nav}
            />
          )}
          {activePage === 'mapping' && (
            <MappingPage
              books={visibleMappingBooks}
              allGenres={genres}
              search={search}
              setSearch={setSearch}
              genreFilter={genreFilter}
              setGenreFilter={setGenreFilter}
              setSortKey={setSortKey}
              patchBook={patchBook}
              nav={nav}
              runJob={runJob}
              jobRunning={jobRunning}
            />
          )}
          {activePage === 'benchmark' && (
            <BenchmarkPage
              books={books}
              shortlisted={shortlisted}
              filters={filters}
              setFilters={setFilters}
              genres={genres}
              activeGenres={activeGenres}
              setActiveGenres={setActiveGenres}
              activeTypes={activeTypes}
              setActiveTypes={setActiveTypes}
              selectedBook={selectedEvalBook}
              setSelectedBook={setSelectedEvalId}
              saveEvaluation={saveEvaluation}
              applyBenchmark={() => applyBenchmark(true)}
              nav={nav}
            />
          )}
          {activePage === 'outreach' && (
            <OutreachPage
              books={outreachBooks}
              selectedBook={selectedOutreachBook}
              setSelectedBook={setSelectedOutreachId}
              emailDraft={emailDraft}
              setEmailDraft={setEmailDraft}
              createDraft={createDraft}
              saveOutreach={saveOutreach}
              sendOutreach={sendOutreach}
              saving={saving}
              nav={nav}
            />
          )}
          {activePage === 'export' && (
            <ExportPage
              books={shortlisted.length ? shortlisted : books}
              metrics={metrics}
              createExport={createExport}
              dataQuality={dataQuality}
            />
          )}
        </main>
      </div>
    </div>
  );
}

function NavItem({
  page,
  activePage,
  onClick,
  icon,
  label,
  badge,
  green,
}: {
  page: PageId;
  activePage: PageId;
  onClick: (page: PageId) => void;
  icon: string;
  label: string;
  badge?: number;
  green?: boolean;
}) {
  return (
    <button className={`nav-item ${activePage === page ? 'active' : ''}`} onClick={() => onClick(page)}>
      <span className="nav-icon">{icon}</span>
      {label}
      {badge !== undefined && <span className={`nav-badge ${green ? 'green' : ''}`}>{badge}</span>}
    </button>
  );
}

function PageHead({ title, desc, children }: { title: string; desc: string; children?: ReactNode }) {
  return (
    <div className="page-head">
      <div>
        <div className="page-title">{title}</div>
        <div className="page-desc">{desc}</div>
      </div>
      <div className="head-actions">{children}</div>
    </div>
  );
}

function JobProgressCard({ job }: { job: Job }) {
  const percent = Math.min(100, Math.max(job.progress_percent || 0, job.status === 'queued' ? 2 : 4));
  const hasKnownTotal = job.progress_total > 0;
  const current = Math.min(job.progress_current || 0, job.progress_total || job.progress_current || 0);
  const statusLabel = job.status === 'queued' ? 'Queued' : 'Running';
  return (
    <div className="job-progress-card" role="status" aria-live="polite">
      <div className="job-progress-head">
        <div>
          <div className="job-progress-kicker">{statusLabel}</div>
          <div className="job-progress-title">{jobStageLabel(job)}</div>
        </div>
        <div className="job-progress-count">
          {hasKnownTotal ? `${current}/${job.progress_total}` : 'Starting'}
        </div>
      </div>
      <div className="job-progress-message">{job.message || 'Preparing job...'}</div>
      <div className={`job-progress-track ${hasKnownTotal ? '' : 'indeterminate'}`}>
        <div className="job-progress-fill" style={{ width: hasKnownTotal ? `${percent}%` : '35%' }} />
      </div>
      <div className="job-progress-meta">
        <span>{hasKnownTotal ? `${Math.round(percent)}%` : 'Waiting for first checkpoint'}</span>
        <span>{job.id.slice(0, 8)}</span>
      </div>
    </div>
  );
}

function DashboardPage({
  books,
  batch,
  runs,
  workspaceId,
  summary,
  metrics,
  genres,
  dataQuality,
  acceptGoodreadsCandidate,
  selectRun,
  createNewRun,
  nav,
}: {
  books: Book[];
  batch: Batch | null;
  runs: Batch[];
  workspaceId: string;
  summary: BatchSummary | null;
  metrics: ReturnType<typeof AppMetrics>;
  genres: string[];
  dataQuality: DataQualityReport | null;
  acceptGoodreadsCandidate: (bookId: number, candidate: GoodreadsCandidate) => void;
  selectRun: (batchId: number) => void;
  createNewRun: () => void;
  nav: (page: PageId) => void;
}) {
  const genreCounts = genres.reduce<Record<string, number>>((acc, genre) => {
    acc[genre] = books.filter((book) => book.genre === genre).length;
    return acc;
  }, {});
  const maxGenre = Math.max(...Object.values(genreCounts), 1);
  const topPicks = [...books].sort((left, right) => audioScore(right) - audioScore(left)).slice(0, 5);
  const totalSources = summary?.total_sources || 0;
  const totalBooks = summary?.total_books || 0;
  const shortlistedBooks = summary?.shortlisted_books || 0;

  return (
    <section className="page active">
      <PageHead title="Commissioning Dashboard" desc="Overview of the current content acquisition pipeline">
        <button className="btn btn-sm" onClick={createNewRun}>+ New Run</button>
        <button className="btn btn-primary btn-sm" onClick={() => nav('benchmark')}>View shortlist →</button>
      </PageHead>

      <div className="runs-panel">
        <div className="runs-panel-head">
          <div>
            <div className="card-title">My Runs</div>
            <div className="run-workspace">Anonymous workspace {workspaceId.slice(-8)}</div>
          </div>
          <button className="btn btn-sm" onClick={createNewRun}>New Run</button>
        </div>
        <div className="run-list">
          {runs.length === 0 && <div className="empty-state">No runs yet</div>}
          {runs.map((run) => (
            <button
              key={run.id}
              className={`run-chip ${batch?.id === run.id ? 'active' : ''}`}
              onClick={() => selectRun(run.id)}
            >
              <span>{run.name || `Run ${run.id}`}</span>
              <small>{new Date(run.updated_at).toLocaleString()}</small>
            </button>
          ))}
        </div>
      </div>

      <div className="pipeline-stages">
        <Stage value={totalSources} label="Links saved" color="var(--p600)" width={totalSources ? 100 : 0} />
        <Stage value={totalBooks} label="Books mapped" color="var(--b600)" width={totalBooks ? 100 : 0} />
        <Stage value={shortlistedBooks} label="Shortlisted" color="var(--t600)" width={totalBooks ? (shortlistedBooks / totalBooks) * 100 : 0} />
        <Stage value={metrics.contacted} label="Outreach sent" color="var(--a400)" width={totalBooks ? (metrics.contacted / totalBooks) * 100 : 0} />
      </div>

      <div className="metrics">
        <Metric icon="★" value={metrics.avgRating.toFixed(1)} label="Avg rating (shortlist)" delta="Live from API" />
        <Metric icon="▤" value={fmtNumber(metrics.avgWord)} label="Avg word count" delta="Estimated audio length" />
        <Metric icon="◉" value={`${Math.round(metrics.avgAudio)}%`} label="Avg audio score" delta="Adaptability score" />
        <Metric icon="@" value={String(metrics.emails)} label="Author emails found" delta={`${Math.max((summary?.total_books || 0) - metrics.emails, 0)} pending lookup`} />
      </div>

      <div className="dashboard-grid">
        <div className="card">
          <div className="card-title">Genre breakdown</div>
          {Object.keys(genreCounts).length === 0 && <div className="empty-state">0 genres mapped</div>}
          {Object.entries(genreCounts)
            .sort(([, left], [, right]) => right - left)
            .map(([genre, count]) => (
              <div className="genre-bar-row" key={genre}>
                <div className="genre-label">{genre}</div>
                <div className="genre-track"><div className="genre-fill" style={{ width: `${(count / maxGenre) * 100}%` }} /></div>
                <span>{count}</span>
              </div>
            ))}
        </div>
        <div className="card">
          <div className="card-title">Top picks this batch <span className="tag tg-t card-tag">Audio-ready</span></div>
          {topPicks.length === 0 && <div className="empty-state">0 books scraped</div>}
          {topPicks.map((book) => (
            <div className="pick-row" key={book.id}>
              <div>
                <div className="pick-title">{book.title}</div>
                <div className="pick-meta">{book.author} · <span className={`tag ${genreTagClass(book.genre)}`}>{book.genre || 'Unmapped'}</span></div>
              </div>
              <AudioScore value={audioScore(book)} />
            </div>
          ))}
        </div>
      </div>

      <DataQualityPanel report={dataQuality} nav={nav} acceptGoodreadsCandidate={acceptGoodreadsCandidate} />

      <div className="card">
        <div className="card-title">Quick actions</div>
        <div className="quick-actions">
          <button className="btn" onClick={() => nav('scrapping')}>↻ Add more links</button>
          <button className="btn" onClick={() => nav('mapping')}>✎ Edit genre mapping</button>
          <button className="btn" onClick={() => nav('benchmark')}>◎ Adjust filters</button>
          <button className="btn" onClick={() => nav('outreach')}>✉ Draft outreach emails</button>
          <button className="btn btn-teal" onClick={() => nav('export')}>⇩ Export final list</button>
        </div>
      </div>
    </section>
  );
}

function DataQualityPanel({
  report,
  nav,
  acceptGoodreadsCandidate,
}: {
  report: DataQualityReport | null;
  nav: (page: PageId) => void;
  acceptGoodreadsCandidate: (bookId: number, candidate: GoodreadsCandidate) => void;
}) {
  if (!report || report.total === 0) {
    return (
      <div className="card">
        <div className="card-title">Data Quality</div>
        <div className="empty-state">Run a scrape to see readiness checks</div>
      </div>
    );
  }
  const coverage = report.coverage || {};
  const issueRows = [...report.rows]
    .filter((row) => row.critical_count || row.warning_count)
    .sort((left, right) => right.critical_count - left.critical_count || right.warning_count - left.warning_count || left.quality_score - right.quality_score)
    .slice(0, 8);
  const genreSources = Object.entries(report.genre_sources || {}).sort(([, left], [, right]) => right - left);
  const goodreadsStatuses = Object.entries(report.goodreads?.status_counts || {}).sort(([, left], [, right]) => right - left);
  const reviewRows = report.rows
    .filter((row) => (row.goodreads_candidates || []).length > 0 && row.goodreads_match_status !== 'matched' && row.goodreads_match_status !== 'accepted')
    .slice(0, 4);
  const keyFields = [
    ['rank', 'best_sellers_rank'],
    ['Goodreads', 'goodreads_rating'],
    ['GR match', 'goodreads_match'],
    ['publisher', 'publisher'],
    ['date', 'publication_date'],
    ['contact', 'contact'],
  ] as const;

  return (
    <div className="card data-quality-card">
      <div className="card-title">
        Data Quality
        <span className={`tag card-tag ${report.ready ? 'tg-g' : 'tg-r'}`}>{report.ready ? 'Export ready' : 'Needs review'}</span>
      </div>
      <div className="quality-summary-grid">
        <Highlight value={`${report.total}`} label="Rows checked" />
        <Highlight value={`${report.critical_count}`} label="Critical issues" variant="amber" />
        <Highlight value={`${report.warning_count}`} label="Warnings" variant="teal" />
      </div>
      <div className="quality-chips">
        {keyFields.map(([label, key]) => (
          <span key={key} className="quality-chip">{label}: {coverage[key] || 0}/{report.total}</span>
        ))}
      </div>
      <div className="quality-split">
        <div>
          <div className="mini-title">Rows needing attention</div>
          {issueRows.length === 0 && <div className="empty-state">No row-level issues detected</div>}
          {issueRows.map((row) => (
            <div className="quality-row" key={row.book_id}>
              <div>
                <strong>{row.title}</strong>
                <small>{row.missing_fields.slice(0, 4).join(', ') || row.issues[0]?.message || 'Check row'}</small>
              </div>
              <span className="tag tg-r">{row.critical_count} / {row.warning_count}</span>
            </div>
          ))}
        </div>
        <div>
          <div className="mini-title">Goodreads coverage</div>
          {goodreadsStatuses.map(([status, count]) => (
            <div className="genre-source-row" key={status}>
              <span>{status.replace(/_/g, ' ')}</span>
              <strong>{count}</strong>
            </div>
          ))}
          <div className="quality-chip gr-confidence">Avg confidence: {Math.round((report.goodreads?.average_confidence || 0) * 100)}%</div>
          <div className="mini-title spaced">Genre source audit</div>
          {genreSources.map(([source, count]) => (
            <div className="genre-source-row" key={source}>
              <span>{source.replace(/_/g, ' ')}</span>
              <strong>{count}</strong>
            </div>
          ))}
          <button className="btn btn-sm quality-action" onClick={() => nav('mapping')}>Review rows</button>
        </div>
      </div>
      {reviewRows.length > 0 && (
        <div className="goodreads-review-queue">
          <div className="mini-title">Goodreads review queue</div>
          {reviewRows.map((row) => {
            const candidate = row.goodreads_candidates?.[0];
            if (!candidate) return null;
            return (
              <div className="goodreads-review-row" key={row.book_id}>
                <div>
                  <strong>{row.title}</strong>
                  <small>{row.goodreads_match_reason || 'Review suggested Goodreads match'}</small>
                  <a href={candidate.url} target="_blank" rel="noreferrer">{candidate.title || candidate.url}</a>
                </div>
                <div className="review-actions">
                  <span className="tag tg-t">{Math.round((candidate.score || 0) * 100)}%</span>
                  <button className="btn btn-xs" onClick={() => acceptGoodreadsCandidate(row.book_id, candidate)}>Accept</button>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function ScrapingPage({
  schemaTab,
  setSchemaTab,
  schemas,
  sourceInputs,
  updateSource,
  toggleSchemaField,
  runJob,
  jobRunning,
  nav,
}: {
  schemaTab: SourceType;
  setSchemaTab: (source: SourceType) => void;
  schemas: Partial<Record<SourceType, StoredSchema>>;
  sourceInputs: Record<'amazon' | 'goodreads', SourceInput[]>;
  updateSource: (source: 'amazon' | 'goodreads', index: number, patch: Partial<SourceInput>) => void;
  toggleSchemaField: (source: SourceType, field: FieldDefinition) => void;
  runJob: (kind: JobKind) => void;
  jobRunning: boolean;
  nav: (page: PageId) => void;
}) {
  return (
    <section className="page active">
      <PageHead title="Scraping" desc="Paste Amazon and Goodreads catalogue URLs. The backend stores sources and runs resumable scrape jobs.">
        <span className="tag tg-g head-tag">Batch input</span>
      </PageHead>

      <div className="card" id="schema-card">
        <div className="card-title">
          Reference sheet mapping
          <div className="spacer" />
          {Object.keys(schemas).length > 0 && <span className="tag tg-t">Loaded from local sheet</span>}
        </div>
        <div className="helper-text">
          The mapped output uses the local reference sheet columns for Amazon, Goodreads, series, and contact fields.
        </div>
        <div className="schema-tabs">
          {(['amazon', 'goodreads', 'shared'] as SourceType[]).map((source) => (
            <button key={source} className={`schema-tab ${schemaTab === source ? 'active' : ''}`} onClick={() => setSchemaTab(source)}>
              {source === 'shared' ? 'Shared / combined' : `${source[0].toUpperCase()}${source.slice(1)} schema`}
            </button>
          ))}
        </div>
        <SchemaPanel
          source={schemaTab}
          schema={schemas[schemaTab]}
          onToggle={toggleSchemaField}
        />
      </div>

      <div className="two-panel">
        <SourceCard source="amazon" title="Amazon catalogue links" badge="Amazon.in / .com" rows={sourceInputs.amazon} updateSource={updateSource} />
        <SourceCard source="goodreads" title="Goodreads catalogue links" badge="Goodreads.com" rows={sourceInputs.goodreads} updateSource={updateSource} />
      </div>

      <div className="action-row">
        <div className="action-note">Fast scrape keeps full Amazon detail accuracy and skips Goodreads until you run enrichment.</div>
        <div className="spacer" />
        <button className="btn" onClick={() => runJob('scrape-fast')} disabled={jobRunning}>↯ {jobRunning ? 'Job running' : 'Fast scrape'}</button>
        <button className="btn" onClick={() => runJob('scrape')} disabled={jobRunning}>↻ {jobRunning ? 'Scraper running' : 'Full scrape'}</button>
        <button className="btn" onClick={() => runJob('enrich-goodreads')} disabled={jobRunning}>◎ Enrich Goodreads</button>
        <button className="btn" onClick={() => runJob('enrich-contacts')} disabled={jobRunning}>@ Find contacts</button>
        <button className="btn btn-primary" onClick={() => nav('mapping')}>Next: Data Mapping →</button>
      </div>
    </section>
  );
}

function SchemaPanel({
  source,
  schema,
  onToggle,
}: {
  source: SourceType;
  schema?: StoredSchema;
  onToggle: (source: SourceType, field: FieldDefinition) => void;
}) {
  const selected = new Set(schema?.selected_fields_json || []);
  return (
    <div className="schema-tab-panel active">
      {!schema && (
        <div className="upload-zone">
          <span className="upload-icon">▣</span>
          <span className="upload-label">Reference columns unavailable</span>
        </div>
      )}
      {schema && (
        <div className="schema-loaded show">
          <div className="schema-file-pill"><span>▤</span><span>{schema.file_name}</span></div>
          <span className="schema-meta">{schema.fields_json.length} fields detected · {selected.size} selected</span>
          <div className="field-grid">
            {schema.fields_json.map((field) => {
              const isOn = field.required || selected.has(field.name);
              return (
                <button key={field.name} className={`field-chip ${field.required ? 'required' : isOn ? 'on' : ''}`} disabled>
                  <span className="fchk">{field.required ? '★' : isOn ? '✓' : '○'}</span>
                  {field.label}
                  <span className="field-type-inline">{field.type}</span>
                </button>
              );
            })}
          </div>
          <details className="schema-details">
            <summary>Show field mapping table</summary>
            <div className="tbl-wrap">
              <table className="schema-preview-table">
                <thead><tr><th>Field name</th><th>Display label</th><th>Type</th><th>Required</th><th>Crawl</th></tr></thead>
                <tbody>
                  {schema.fields_json.map((field) => {
                    const isOn = field.required || selected.has(field.name);
                    return (
                      <tr key={field.name}>
                        <td><code>{field.name}</code></td>
                        <td>{field.label}</td>
                        <td><span className="field-type-badge">{field.type}</span></td>
                        <td><span className="field-req-dot" style={{ background: field.required ? 'var(--t600)' : 'var(--g100)' }} /></td>
                        <td>{isOn ? '✓' : '-'}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </details>
        </div>
      )}
    </div>
  );
}

function SourceCard({
  source,
  title,
  badge,
  rows,
  updateSource,
}: {
  source: 'amazon' | 'goodreads';
  title: string;
  badge: string;
  rows: SourceInput[];
  updateSource: (source: 'amazon' | 'goodreads', index: number, patch: Partial<SourceInput>) => void;
}) {
  return (
    <div className="card">
      <div className="card-title"><span className={`source-dot ${source}`} /> {title}<span className={`tag ${source === 'amazon' ? 'tg-a' : 'tg-t'} card-tag`}>{badge}</span></div>
      <div className="helper-text">Paste bestseller lists, category pages, shelf pages, or author pages.</div>
      <div className="link-group">
        {rows.map((row, index) => (
          <div className="link-row" key={`${source}-${index}`}>
            <div className="link-num">{index + 1}</div>
            <div className={`link-status ${row.url ? 'ok' : ''}`} />
            <input type="url" value={row.url} placeholder={`Paste ${source} URL...`} onChange={(event) => updateSource(source, index, { url: event.target.value })} />
          </div>
        ))}
      </div>
      <div className="source-options">
        <label className="field-label">Output format</label>
        <select value={rows[0]?.output_format || 'CSV'} onChange={(event) => rows.forEach((_, index) => updateSource(source, index, { output_format: event.target.value }))}>
          <option>CSV</option><option>JSON</option><option>XLSX</option>
        </select>
      </div>
    </div>
  );
}

function MappingPage({
  books,
  allGenres,
  search,
  setSearch,
  genreFilter,
  setGenreFilter,
  setSortKey,
  patchBook,
  nav,
  runJob,
  jobRunning,
}: {
  books: Book[];
  allGenres: string[];
  search: string;
  setSearch: (value: string) => void;
  genreFilter: string;
  setGenreFilter: (value: string) => void;
  setSortKey: (value: 'title' | 'author' | 'rating' | 'rating_count' | 'word_count') => void;
  patchBook: (bookId: number, patch: Partial<Book>) => void;
  nav: (page: PageId) => void;
  runJob: (kind: JobKind) => void;
  jobRunning: boolean;
}) {
  const genreOptions = Array.from(
    new Set([
      'Thriller',
      'Mystery',
      'Romance',
      'Fantasy',
      'Sci-Fi',
      'Historical',
      'Contemporary',
      'Satire',
      ...allGenres,
      ...books.map((book) => book.genre || '').filter(Boolean),
    ]),
  ).filter(Boolean);

  return (
    <section className="page active">
      <PageHead title="Data Cleaning & Genre Mapping" desc="Review scraped records, assign taxonomy, and verify audio adaptability scores.">
        <button className="btn btn-sm" onClick={() => runJob('enrich-goodreads')} disabled={jobRunning}>◎ Enrich Goodreads</button>
        <button className="btn btn-primary btn-sm" onClick={() => nav('benchmark')}>Proceed to filters →</button>
      </PageHead>

      <div className="metrics">
        <Metric icon="▤" value={String(books.length)} label="Visible books" delta="Filtered table rows" />
        <Metric icon="◇" value={String(books.filter((book) => book.genre).length)} label="Genres mapped" delta={`${books.filter((book) => !book.genre).length} need review`} />
        <Metric icon="§" value={String(books.filter((book) => book.book_type === 'Series').length)} label="Series found" delta={`${books.filter((book) => book.book_type !== 'Series').length} standalone`} />
        <Metric icon="◉" value={`${Math.round(books.reduce((sum, book) => sum + audioScore(book), 0) / Math.max(books.length, 1))}%`} label="Avg audio score" delta="Current table" />
      </div>

      <div className="card">
        <div className="card-title">Mapping configuration</div>
        <div className="four-col">
          <FieldSelect label="Primary genre source" options={['Amazon categories', 'Goodreads shelves', 'Combined vote']} />
          <FieldSelect label="Sub-genre basis" options={['Scraped sub-categories', 'Goodreads genres', 'Manual']} />
          <FieldSelect label="Secondary type" options={['Series / Standalone / Anthology', 'Trope-based', 'Age category']} />
          <FieldSelect label="Synopsis from" options={['Merge both sources', 'Amazon only', 'Goodreads only']} />
        </div>
      </div>

      <div className="toolbar">
        <div className="search-wrap">
          <span className="search-icon">⌕</span>
          <input className="search-input" value={search} placeholder="Search by title, author, genre..." onChange={(event) => setSearch(event.target.value)} />
        </div>
        <select value={genreFilter} onChange={(event) => setGenreFilter(event.target.value)}>
          <option value="">All genres</option>
          {allGenres.map((genre) => <option key={genre}>{genre}</option>)}
        </select>
      </div>

      <div className="card table-card">
        <div className="tbl-wrap">
          <table>
            <thead>
              <tr>
                <th><input type="checkbox" /></th>
                <th onClick={() => setSortKey('title')}>Title <span className="sort-arrow">▲</span></th>
                <th onClick={() => setSortKey('author')}>Author <span className="sort-arrow">▲</span></th>
                <th>Source</th>
                <th onClick={() => setSortKey('rating')}>Rating <span className="sort-arrow">▲</span></th>
                <th onClick={() => setSortKey('rating_count')}>Reviews <span className="sort-arrow">▲</span></th>
                <th>Primary genre</th>
                <th>Sub-genre</th>
                <th>Type</th>
                <th>Series books</th>
                <th>Audio score</th>
                <th onClick={() => setSortKey('word_count')}>Word count <span className="sort-arrow">▲</span></th>
                <th>Synopsis</th>
                <th>Author email</th>
              </tr>
            </thead>
            <tbody>
              {books.map((book) => (
                <tr key={book.id}>
                  <td><input type="checkbox" defaultChecked={book.shortlisted} /></td>
                  <td><div className="cell-title" title={book.title}>{book.title}</div></td>
                  <td>{book.author}</td>
                  <td><span className={`tag ${sourceFor(book) === 'Amazon' ? 'tg-a' : 'tg-t'}`}>{sourceFor(book)}</span></td>
                  <td>{book.rating?.toFixed(1) || '-'}</td>
                  <td>{fmtNumber(book.rating_count)}</td>
                  <td>
                    <select className="tbl-select" value={book.genre || ''} onChange={(event) => patchBook(book.id, { genre: event.target.value })}>
                      <option value="">Unmapped</option>
                      {genreOptions.map((genre) => <option key={genre}>{genre}</option>)}
                    </select>
                  </td>
                  <td><input className="table-input" value={book.sub_genre || ''} onChange={(event) => patchBook(book.id, { sub_genre: event.target.value })} /></td>
                  <td>
                    <select className="tbl-select" value={book.book_type || ''} onChange={(event) => patchBook(book.id, { book_type: event.target.value })}>
                      <option value="">Unknown</option><option>Series</option><option>Standalone</option><option>Anthology</option>
                    </select>
                  </td>
                  <td>{seriesCount(book)}</td>
                  <td><AudioScore value={audioScore(book)} /></td>
                  <td>{fmtNumber(wordCount(book))}</td>
                  <td className="cell-synopsis">{book.synopsis || '-'}</td>
                  <td className="email-cell">{contactEmail(book) || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="action-row">
        <button className="btn" onClick={() => nav('scrapping')}>← Back to scraping</button>
        <div className="spacer" />
        <button className="btn btn-primary" onClick={() => nav('benchmark')}>Next: Benchmark Filters →</button>
      </div>
    </section>
  );
}

function BenchmarkPage({
  books,
  shortlisted,
  filters,
  setFilters,
  genres,
  activeGenres,
  setActiveGenres,
  activeTypes,
  setActiveTypes,
  selectedBook,
  setSelectedBook,
  saveEvaluation,
  applyBenchmark,
  nav,
}: {
  books: Book[];
  shortlisted: Book[];
  filters: Filters;
  setFilters: (filters: Filters) => void;
  genres: string[];
  activeGenres: string[];
  setActiveGenres: (values: string[]) => void;
  activeTypes: string[];
  setActiveTypes: (values: string[]) => void;
  selectedBook?: Book;
  setSelectedBook: (id: number) => void;
  saveEvaluation: (book: Book, patch: Partial<Evaluation>) => void;
  applyBenchmark: () => void;
  nav: (page: PageId) => void;
}) {
  return (
    <section className="page active">
      <PageHead title="Benchmark Filters" desc="Tune quantitative and qualitative criteria to arrive at your final shortlist.">
        <span className="tag tg-t head-tag">{shortlisted.length} shortlisted</span>
        <button className="btn btn-primary btn-sm" onClick={() => nav('outreach')}>Next: Outreach →</button>
      </PageHead>

      <div className="benchmark-layout">
        <div>
          <div className="card">
            <div className="card-title">Quantitative filters</div>
            <Slider label="Min rating" value={filters.min_rating} min={1} max={5} step={0.1} suffix=" ★" onChange={(value) => setFilters({ ...filters, min_rating: value })} />
            <Slider label="Min reviews" value={filters.min_reviews} min={0} max={200000} step={1000} display={`${fmtNumber(filters.min_reviews)}+`} onChange={(value) => setFilters({ ...filters, min_reviews: value })} />
            <Slider label="Min word count" value={filters.min_word_count} min={10000} max={300000} step={5000} display={`${fmtNumber(filters.min_word_count)}+`} onChange={(value) => setFilters({ ...filters, min_word_count: value })} />
            <Slider label="Max books in series" value={filters.max_series_books} min={1} max={20} step={1} display={`≤ ${filters.max_series_books}`} onChange={(value) => setFilters({ ...filters, max_series_books: value })} />
            <Slider label="Min audio score" value={filters.min_audio_score} min={0} max={100} step={5} display={`${filters.min_audio_score}+`} onChange={(value) => setFilters({ ...filters, min_audio_score: value })} />
            <hr />
            <ChipGroup label="Genre" values={genres} active={activeGenres} setActive={setActiveGenres} />
            <ChipGroup label="Type" values={['Series', 'Standalone', 'Anthology']} active={activeTypes} setActive={setActiveTypes} />
            <button className="btn btn-primary full-btn" onClick={applyBenchmark}>Apply filters</button>
          </div>
          <SeriesEstimator />
        </div>

        <div>
          <div className="card">
            <div className="card-title">Shortlisted books <span className="tag tg-t">{shortlisted.length} books</span></div>
            <div className="book-grid">
              {books.map((book) => (
                <button key={book.id} className={`book-card ${book.shortlisted ? 'sel' : 'filtered'} ${selectedBook?.id === book.id ? 'focused' : ''}`} onClick={() => setSelectedBook(book.id)}>
                  {book.shortlisted && <div className="sel-check">✓</div>}
                  <div className="book-card-title" title={book.title}>{book.title}</div>
                  <div className="book-card-author">{book.author}</div>
                  <div className="book-card-tags"><span className={`tag ${genreTagClass(book.genre)}`}>{book.genre || 'Unmapped'}</span><span className="tag tg-g">{book.rating?.toFixed(1) || '-'}★</span></div>
                  <AudioScore value={audioScore(book)} />
                  <div className="book-card-stat">{fmtNumber(wordCount(book))} words · {fmtNumber(book.rating_count)} reviews</div>
                </button>
              ))}
            </div>
          </div>
          {selectedBook && (
            <div className="card">
              <div className="card-title">Subjective evaluation <span className="muted-title">- {selectedBook.title}, {selectedBook.author}</span></div>
              <StarRow label="Story & narrative" value={selectedBook.evaluation?.story_score || 3} onChange={(value) => saveEvaluation(selectedBook, { story_score: value })} />
              <StarRow label="Characters" value={selectedBook.evaluation?.characters_score || 3} onChange={(value) => saveEvaluation(selectedBook, { characters_score: value })} />
              <StarRow label="High points / hooks" value={selectedBook.evaluation?.hooks_score || 3} onChange={(value) => saveEvaluation(selectedBook, { hooks_score: value })} />
              <StarRow label="Series / episode potential" value={selectedBook.evaluation?.series_potential_score || 3} onChange={(value) => saveEvaluation(selectedBook, { series_potential_score: value })} />
              <StarRow label="Audio adaptability" value={selectedBook.evaluation?.audio_adaptability_score || 3} onChange={(value) => saveEvaluation(selectedBook, { audio_adaptability_score: value })} />
              <StarRow label="India audience fit" value={selectedBook.evaluation?.india_fit_score || 3} onChange={(value) => saveEvaluation(selectedBook, { india_fit_score: value })} />
              <div className="field-row">
                <label className="field-label">Commissioning notes / high points</label>
                <textarea className="email-body compact" defaultValue={selectedBook.evaluation?.notes || ''} onBlur={(event) => saveEvaluation(selectedBook, { notes: event.target.value })} />
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="action-row">
        <button className="btn" onClick={() => nav('mapping')}>← Back to mapping</button>
        <div className="spacer" />
        <button className="btn btn-primary" onClick={() => nav('outreach')}>Next: Author Outreach →</button>
      </div>
    </section>
  );
}

function OutreachPage({
  books,
  selectedBook,
  setSelectedBook,
  emailDraft,
  setEmailDraft,
  createDraft,
  saveOutreach,
  sendOutreach,
  saving,
  nav,
}: {
  books: Book[];
  selectedBook?: Book;
  setSelectedBook: (id: number) => void;
  emailDraft: { recipient: string; cc: string; subject: string; body: string };
  setEmailDraft: (draft: { recipient: string; cc: string; subject: string; body: string }) => void;
  createDraft: (template: string) => void;
  saveOutreach: (status?: string) => void;
  sendOutreach: () => void;
  saving: boolean;
  nav: (page: PageId) => void;
}) {
  const sent = books.filter((book) => book.outreach_messages?.some((message) => message.status === 'sent')).length;
  return (
    <section className="page active">
      <PageHead title="Author Outreach" desc="Draft and track commissioning emails to authors, publishers, or literary agents.">
        <span className="tag tg-a head-tag">{sent} of {books.length} contacted</span>
      </PageHead>
      <div className="outreach-layout">
        <div className="card list-card">
          <div className="list-title">Shortlisted titles</div>
          <div className="outreach-list">
            {books.map((book) => {
              const status = book.outreach_messages?.[0]?.status || 'not sent';
              return (
                <button key={book.id} className={`outreach-row ${selectedBook?.id === book.id ? 'active' : ''}`} onClick={() => setSelectedBook(book.id)}>
                  <div className="book-card-title">{book.title}</div>
                  <div className="book-card-author">{book.author}</div>
                  <span className={`tag ${status === 'sent' ? 'tg-gr' : status === 'draft' ? 'tg-a' : 'tg-g'}`}>{status === 'sent' ? '✓ Sent' : status}</span>
                </button>
              );
            })}
          </div>
        </div>
        <div className="card">
          <div className="card-title">Email draft <div className="spacer" /><button className="btn btn-xs" onClick={() => createDraft('formal')}>Formal</button><button className="btn btn-xs" onClick={() => createDraft('casual')}>Casual</button><button className="btn btn-xs" onClick={() => createDraft('rights')}>Rights inquiry</button></div>
          <EmailField label="To" value={emailDraft.recipient} onChange={(value) => setEmailDraft({ ...emailDraft, recipient: value })} />
          <EmailField label="CC" value={emailDraft.cc} onChange={(value) => setEmailDraft({ ...emailDraft, cc: value })} />
          <EmailField label="Subject" value={emailDraft.subject} onChange={(value) => setEmailDraft({ ...emailDraft, subject: value })} />
          <textarea className="email-body" value={emailDraft.body} onChange={(event) => setEmailDraft({ ...emailDraft, body: event.target.value })} />
          <div className="composer-actions">
            <button className="btn btn-primary" disabled={saving} onClick={sendOutreach}>✉ Send email</button>
            <button className="btn" disabled={saving} onClick={() => saveOutreach('draft')}>▣ Save draft</button>
            <button className="btn" onClick={() => navigator.clipboard?.writeText(emailDraft.body)}>▤ Copy</button>
          </div>
          <div className="tracker">
            <div className="tracker-title">Outreach tracker</div>
            {books.filter((book) => book.outreach_messages?.length).map((book) => (
              <div className="tracker-row" key={book.id}><span>{book.title}</span><span>{book.outreach_messages?.[0]?.status}</span></div>
            ))}
          </div>
        </div>
      </div>
      <div className="action-row">
        <button className="btn" onClick={() => nav('benchmark')}>← Back to filters</button>
        <div className="spacer" />
        <button className="btn btn-primary" onClick={() => nav('export')}>Next: Export →</button>
      </div>
    </section>
  );
}

function ExportPage({
  books,
  metrics,
  createExport,
  dataQuality,
}: {
  books: Book[];
  metrics: ReturnType<typeof AppMetrics>;
  createExport: (format: 'csv' | 'xlsx' | 'pdf' | 'json', profile?: string) => void;
  dataQuality: DataQualityReport | null;
}) {
  const exportOptions = [
    { label: 'Sample-compatible CSV', format: 'csv' as const, profile: 'sample_compatible', detail: 'Exact 49-column benchmark sheet contract' },
    { label: 'Full diagnostic CSV', format: 'csv' as const, profile: 'full_diagnostic', detail: 'Includes provenance and data-quality columns' },
    { label: 'JSON diagnostic', format: 'json' as const, profile: 'full_diagnostic', detail: 'Structured rows for audit or automation' },
  ];
  return (
    <section className="page active">
      <PageHead title="Export & Share" desc="Download the final shortlist and evaluation pack for your commissioning team." />
      <div className="highlight-row">
        <Highlight value={String(books.length)} label="Books in final shortlist" />
        <Highlight value={fmtNumber(metrics.avgWord)} label="Avg word count" variant="teal" />
        <Highlight value={`${metrics.avgRating.toFixed(1)} ★`} label="Avg audience rating" variant="amber" />
      </div>
      <div className="two-panel">
        <div className="card">
          <div className="card-title">Download formats</div>
          {exportOptions.map((option) => (
            <button key={`${option.format}-${option.profile}`} className="download-row" onClick={() => createExport(option.format, option.profile)}>
              <span>{option.format === 'csv' ? '▤' : '{ }'}</span>
              <div><strong>{option.label}</strong><small>{books.length} rows · {option.detail}</small></div>
            </button>
          ))}
          {dataQuality && (
            <div className={`export-readiness ${dataQuality.ready ? 'ready' : 'review'}`}>
              {dataQuality.ready ? 'No critical data-quality issues detected.' : `${dataQuality.critical_count} critical data-quality issues remain before final handoff.`}
            </div>
          )}
        </div>
        <div className="card">
          <div className="card-title">Export field selection</div>
          <div className="checkbox-grid">
            {['Title & author', 'Rating & reviews', 'Primary genre', 'Sub-genre', 'Type', 'Word count', 'Audio score', 'Synopsis', 'Author email', 'Series book count', 'Publisher name', 'Subjective eval score'].map((field) => (
              <label key={field}><input type="checkbox" defaultChecked={field !== 'Publisher name'} /> {field}</label>
            ))}
          </div>
          <hr />
          <div className="card-title">Share with team</div>
          <div className="field-row"><label className="field-label">Send to email</label><input type="email" placeholder="colleague@pocketfm.com" /></div>
          <div className="field-row"><label className="field-label">Add a note</label><textarea className="small-textarea" placeholder="Notes for the recipient..." /></div>
          <button className="btn btn-teal">✉ Share via email</button>
        </div>
      </div>
      <div className="card table-card">
        <div className="table-head">
          <span>Shortlist preview</span>
          <span className="tag tg-t">{books.length} books</span>
          <div className="spacer" />
          <span className="cell-muted" style={{ fontSize: 12 }}>Pick a format card above to generate an export</span>
        </div>
        <div className="tbl-wrap">
          <table>
            <thead><tr><th>#</th><th>Title</th><th>Author</th><th>Genre</th><th>Rating</th><th>Word count</th><th>Audio score</th><th>Type</th><th>Eval score</th></tr></thead>
            <tbody>
              {books.map((book, index) => (
                <tr key={book.id}>
                  <td>{index + 1}</td>
                  <td><div className="cell-title">{book.title}</div><div className="cell-muted">{book.author}</div></td>
                  <td>{book.author}</td>
                  <td><span className={`tag ${genreTagClass(book.genre)}`}>{book.genre || 'Unmapped'}</span></td>
                  <td>{book.rating?.toFixed(1) || '-'}</td>
                  <td>{fmtNumber(wordCount(book))}</td>
                  <td><AudioScore value={audioScore(book)} /></td>
                  <td><span className={`tag ${book.book_type === 'Series' ? 'tg-p' : 'tg-g'}`}>{book.book_type || '-'}</span></td>
                  <td>{evaluationScore(book)}/30</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

function Metric({ icon, value, label, delta }: { icon: string; value: string; label: string; delta: string }) {
  return <div className="metric"><div className="metric-icon">{icon}</div><div className="metric-val">{value}</div><div className="metric-label">{label}</div><div className="metric-delta">{delta}</div></div>;
}

function Stage({ value, label, color, width }: { value: number; label: string; color: string; width: number }) {
  return <div className="pipe-stage"><div className="pipe-stage-num">{value}</div><div className="pipe-stage-label">{label}</div><div className="pipe-bar"><div className="pipe-bar-fill" style={{ width: `${width}%`, background: color }} /></div></div>;
}

function Highlight({ value, label, variant = '' }: { value: string; label: string; variant?: string }) {
  return <div className={`highlight ${variant}`}><div className="highlight-num">{value}</div><div className="highlight-label">{label}</div></div>;
}

function AudioScore({ value }: { value: number }) {
  const cls = value >= 80 ? 'score-hi' : value >= 60 ? 'score-md' : 'score-lo';
  return <div className="audio-score"><div className="audio-bar"><div className={`audio-fill ${cls}`} style={{ width: `${value ? Math.max(value, 4) : 0}%` }} /></div><span>{value || '-'}</span></div>;
}

function FieldSelect({ label, options }: { label: string; options: string[] }) {
  return <div><label className="field-label">{label}</label><select>{options.map((option) => <option key={option}>{option}</option>)}</select></div>;
}

function Slider({ label, value, min, max, step, suffix = '', display, onChange }: { label: string; value: number; min: number; max: number; step: number; suffix?: string; display?: string; onChange: (value: number) => void }) {
  return (
    <>
      <div className="slider-row"><div className="slider-label">{label}</div></div>
      <div className="slider-row slider-control"><input type="range" min={min} max={max} step={step} value={value} onChange={(event) => onChange(Number(event.target.value))} /><div className="slider-val">{display || `${value}${suffix}`}</div></div>
    </>
  );
}

function ChipGroup({ label, values, active, setActive }: { label: string; values: string[]; active: string[]; setActive: (values: string[]) => void }) {
  function toggle(value: string) {
    setActive(active.includes(value) ? active.filter((item) => item !== value) : [...active, value]);
  }
  return (
    <>
      <div className="chip-label">{label}</div>
      <div className="chips">
        <button className={`chip ${active.length === 0 ? 'on' : ''}`} onClick={() => setActive([])}>All</button>
        {values.map((value) => <button key={value} className={`chip ${active.includes(value) ? 'on' : ''}`} onClick={() => toggle(value)}>{value}</button>)}
      </div>
    </>
  );
}

function SeriesEstimator() {
  const [books, setBooks] = useState(5);
  const [pages, setPages] = useState(320);
  const [words, setWords] = useState(250);
  const total = books * pages * words;
  return (
    <div className="card">
      <div className="card-title">Series length estimator</div>
      <div className="field-row"><label className="field-label">No. of primary books</label><input type="number" value={books} onChange={(event) => setBooks(Number(event.target.value) || 0)} /></div>
      <div className="field-row"><label className="field-label">Avg pages per book</label><input type="number" value={pages} onChange={(event) => setPages(Number(event.target.value) || 0)} /></div>
      <div className="field-row"><label className="field-label">Words per page</label><input type="number" value={words} onChange={(event) => setWords(Number(event.target.value) || 0)} /></div>
      <div className="estimate-box"><div>Estimated total word count</div><strong>{total.toLocaleString()}</strong><span>≈ {Math.round(total / 9000)} hrs of audio</span></div>
    </div>
  );
}

function StarRow({ label, value, onChange }: { label: string; value: number; onChange: (value: number) => void }) {
  return (
    <div className="star-row">
      <div className="star-label">{label}</div>
      <div className="star-group">
        {[1, 2, 3, 4, 5].map((score) => <button key={score} className={`star-btn ${score <= value ? 'on' : ''}`} onClick={() => onChange(score)}>★</button>)}
      </div>
    </div>
  );
}

function EmailField({ label, value, onChange }: { label: string; value: string; onChange: (value: string) => void }) {
  return <div className="email-field"><span className="email-field-label">{label}</span><input value={value} onChange={(event) => onChange(event.target.value)} /></div>;
}

function buildLocalTemplate(book: Book, template: string) {
  if (template === 'rights') {
    return `Dear Rights Team,\n\nThis is a formal inquiry regarding audio rights for ${book.title} by ${book.author}.\n\nRegards,\nAstha Singh\nContent Commissioning - Pocket FM`;
  }
  if (template === 'casual') {
    return `Hi ${book.author}'s team,\n\nWriting from Pocket FM because ${book.title} looks like a strong fit for audio adaptation.\n\nBest,\nAstha Singh`;
  }
  return `Dear ${book.author}'s Literary Team,\n\nI'm reaching out from Pocket FM regarding ${book.title}. We would love to explore audio rights and commissioning possibilities.\n\nWarm regards,\nAstha Singh\nastha.singh@pocketfm.com`;
}

function evaluationScore(book: Book): number {
  const evaluation = book.evaluation || {};
  return ['story_score', 'characters_score', 'hooks_score', 'series_potential_score', 'audio_adaptability_score', 'india_fit_score'].reduce((sum, key) => {
    const value = evaluation[key as keyof Evaluation];
    return sum + (typeof value === 'number' ? value : 0);
  }, 0);
}

function AppMetrics() {
  return { total: 0, avgRating: 0, avgWord: 0, avgAudio: 0, emails: 0, contacted: 0 };
}

export default App;
