import React from "react";
import {
  AlertCircle,
  CheckCircle2,
  Cloud,
  Database,
  Eye,
  Play,
  RefreshCw,
  Save,
  Trash2,
} from "lucide-react";
import Icon from "../../ui/Icon.jsx";
import { statusLabel } from "./model.js";

function Field({ label, children }) {
  return (
    <label className="flex min-w-0 flex-1 flex-col gap-2 text-sm text-secondary">
      {label}
      {children}
    </label>
  );
}

function StatusMark({ configured }) {
  return configured ? (
    <Icon type={CheckCircle2} className="text-accent" size={18} />
  ) : (
    <Icon type={AlertCircle} className="text-negative" size={18} />
  );
}

export default function DataPage({ ingestion }) {
  const { form, updateForm, status, lists, jobs, preview, message } = ingestion;
  return (
    <main className="flex-1 overflow-y-auto px-8 py-7">
      <div className="mx-auto max-w-6xl space-y-6">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="flex items-center gap-3 text-accent">
              <Icon type={Database} size={22} />
              <span className="text-sm font-medium uppercase tracking-[0.16em]">
                Market data
              </span>
            </div>
            <h1 className="mt-2 text-3xl font-semibold">Data ingestion</h1>
            <p className="mt-2 max-w-2xl text-secondary">
              Backfill one-minute, unadjusted stock bars to your AWS S3 dataset
              and keep recent dates refreshed daily.
            </p>
          </div>
          <div className="rounded-lg border border-line bg-panel px-4 py-3 text-sm">
            <div className="flex items-center gap-2">
              <StatusMark configured={status.configured} />
              <span>
                {status.configured ? "AWS connected" : "AWS setup required"}
              </span>
            </div>
            <p className="mt-1 text-xs text-muted">
              {status.provider} · {status.region}
            </p>
          </div>
        </div>

        <section className="rounded-xl border border-line bg-panel p-5">
          <div className="flex items-center gap-3">
            <Icon type={Cloud} className="text-accent" />
            <div>
              <h2 className="text-lg font-semibold">Connection</h2>
              <p className="text-sm text-secondary">{status.message}</p>
            </div>
          </div>
          {!status.configured && (
            <p className="mt-4 rounded-lg border border-yellow-700/50 bg-yellow-950/20 px-3 py-2 text-sm text-yellow-200">
              Set BACKTEST_INGESTION_CONTROL_ARN and configure shared AWS
              credentials. Provider keys stay in AWS Secrets Manager and never
              enter this page.
            </p>
          )}
        </section>

        <section className="rounded-xl border border-line bg-panel p-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">Backfill data</h2>
              <p className="text-sm text-secondary">
                Preview work before creating a cloud job.
              </p>
            </div>
            <span className="badge">S3 · Parquet · 1 minute</span>
          </div>
          <div className="mt-5 grid gap-4 md:grid-cols-2">
            <Field label="Symbols">
              <input
                className="control"
                aria-label="Ingestion symbols"
                value={form.symbols}
                onChange={(event) => updateForm("symbols", event.target.value)}
                placeholder="AAL, MSFT, NVDA"
              />
            </Field>
            <Field label="Saved list name">
              <div className="flex gap-2">
                <input
                  className="control min-w-0"
                  aria-label="Saved list name"
                  value={form.listName}
                  onChange={(event) =>
                    updateForm("listName", event.target.value)
                  }
                  placeholder="US large caps"
                />
                <button
                  className="button shrink-0"
                  onClick={ingestion.saveList}
                  disabled={!form.listName.trim()}
                >
                  <Icon type={Save} size={16} /> Save list
                </button>
              </div>
            </Field>
            <Field label="Start date">
              <input
                className="control"
                aria-label="Ingestion start date"
                type="date"
                value={form.start}
                onChange={(event) => updateForm("start", event.target.value)}
              />
            </Field>
            <Field label="End date">
              <input
                className="control"
                aria-label="Ingestion end date"
                type="date"
                value={form.end}
                onChange={(event) => updateForm("end", event.target.value)}
              />
            </Field>
          </div>
          <div className="mt-5 flex flex-wrap items-center gap-3">
            <button className="button" onClick={ingestion.previewBackfill}>
              <Icon type={Eye} size={16} /> Preview range
            </button>
            <button
              className="button primary"
              onClick={ingestion.startBackfill}
            >
              <Icon type={Play} size={16} /> Start backfill
            </button>
            {preview && (
              <span className="text-sm text-accent">
                {preview.units} symbol/date units ·{" "}
                {preview.symbols?.length || 0} symbols · {preview.start} to{" "}
                {preview.end}
              </span>
            )}
          </div>
          {message && (
            <p
              role="status"
              className={`mt-4 text-sm ${message.type === "error" ? "text-negative" : "text-accent"}`}
            >
              {message.text}
            </p>
          )}
        </section>

        <section className="rounded-xl border border-line bg-panel p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">Saved symbol lists</h2>
              <p className="text-sm text-secondary">
                Lists are stored in DynamoDB when AWS is configured.
              </p>
            </div>
            <button
              className="icon-button"
              aria-label="Refresh ingestion status"
              title="Refresh"
              onClick={ingestion.refresh}
            >
              <Icon type={RefreshCw} size={17} />
            </button>
          </div>
          {lists.length ? (
            <div className="mt-4 grid gap-2 sm:grid-cols-2">
              {lists.map((list) => (
                <div
                  key={list.id}
                  className="flex items-center justify-between gap-3 rounded-lg border border-line px-3 py-2"
                >
                  <div>
                    <div className="font-medium">{list.name}</div>
                    <div className="text-sm text-secondary">
                      {list.symbols.join(", ")}
                    </div>
                  </div>
                  <button
                    className="icon-button"
                    aria-label={`Delete ${list.name}`}
                    onClick={() => ingestion.deleteList(list.id)}
                  >
                    <Icon type={Trash2} size={16} />
                  </button>
                </div>
              ))}
            </div>
          ) : (
            <p className="mt-4 text-sm text-muted">No saved lists available.</p>
          )}
        </section>

        <section className="rounded-xl border border-line bg-panel p-5">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold">Ingestion jobs</h2>
              <p className="text-sm text-secondary">
                Daily refresh scheduling and recovery appear here after AWS
                setup.
              </p>
            </div>
            <div className="flex items-center gap-3">
              <label className="flex items-center gap-2 text-sm text-secondary">
                <input
                  type="checkbox"
                  aria-label="Enable daily refresh"
                  checked={ingestion.scheduleEnabled}
                  onChange={(event) =>
                    ingestion.setSchedule(event.target.checked)
                  }
                />
                Daily refresh
              </label>
              <span className="badge">06:00 America/New_York</span>
            </div>
          </div>
          {jobs.length ? (
            <div className="mt-4 overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="text-muted">
                  <tr>
                    <th className="px-3 py-2">Status</th>
                    <th className="px-3 py-2">Coverage</th>
                    <th className="px-3 py-2">Rows</th>
                    <th className="px-3 py-2">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.map((job) => (
                    <tr key={job.id} className="border-t border-line">
                      <td className="px-3 py-3">{statusLabel(job.status)}</td>
                      <td className="px-3 py-3">
                        {job.completed_units} / {job.units} units
                      </td>
                      <td className="px-3 py-3">{job.rows}</td>
                      <td className="flex gap-3 px-3 py-3">
                        {["queued", "running", "waiting"].includes(
                          job.status,
                        ) && (
                          <button
                            className="text-accent"
                            onClick={() => ingestion.cancelJob(job.id)}
                          >
                            Cancel
                          </button>
                        )}
                        {["failed", "needs-attention", "cancelled"].includes(
                          job.status,
                        ) && (
                          <button
                            className="text-accent"
                            onClick={() => ingestion.retryJob(job.id)}
                          >
                            Retry
                          </button>
                        )}
                        {job.discrepancies?.length > 0 && (
                          <button
                            className="text-yellow-200"
                            onClick={() =>
                              ingestion.reviewDiscrepancy(
                                job.discrepancies[0].id,
                              )
                            }
                          >
                            Review omissions
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="mt-4 text-sm text-muted">No ingestion jobs yet.</p>
          )}
        </section>

        <section className="rounded-xl border border-red-900/60 bg-red-950/10 p-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-3">
              <Icon type={Trash2} className="text-negative" />
              <div>
                <h2 className="text-lg font-semibold">Delete stored data</h2>
                <p className="text-sm text-secondary">
                  Deletion is an explicit range action and prevents scheduled
                  recreation.
                </p>
              </div>
            </div>
            <button className="button" onClick={ingestion.deleteData}>
              <Icon type={Trash2} size={16} />
              Delete selected range
            </button>
          </div>
        </section>
      </div>
    </main>
  );
}
