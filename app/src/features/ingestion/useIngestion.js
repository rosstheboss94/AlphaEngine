import { useCallback, useEffect, useState } from "react";
import {
  initialIngestionForm,
  localPreview,
  parseSymbols,
  validateIngestionForm,
} from "./model.js";

function getBridge() {
  return globalThis.window?.go?.main?.App || null;
}

async function invoke(method, ...args) {
  const bridge = getBridge();
  if (!bridge || typeof bridge[method] !== "function") {
    throw new Error("The desktop bridge is unavailable in browser preview.");
  }
  return bridge[method](...args);
}

export function useIngestion(active = true) {
  const [form, setForm] = useState(initialIngestionForm);
  const [status, setStatus] = useState({
    provider: "Massive",
    mode: "loading",
    configured: false,
    region: "us-east-1",
    message: "Checking AWS connection...",
  });
  const [lists, setLists] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [preview, setPreview] = useState(null);
  const [message, setMessage] = useState(null);

  const refresh = useCallback(async () => {
    try {
      setStatus(await invoke("IngestionStatus"));
    } catch (error) {
      setStatus({
        provider: "Massive",
        mode: "setup-required",
        configured: false,
        region: "us-east-1",
        message: error.message,
      });
    }
    try {
      setLists((await invoke("IngestionLists")) || []);
      setJobs((await invoke("IngestionJobs")) || []);
    } catch {
      setLists([]);
      setJobs([]);
    }
  }, []);

  useEffect(() => {
    if (!active) return;
    refresh();
  }, [active, refresh]);

  useEffect(() => {
    if (
      !active ||
      !jobs.some((job) => ["queued", "running", "waiting"].includes(job.status))
    )
      return undefined;
    const timer = setInterval(refresh, 5000);
    return () => clearInterval(timer);
  }, [active, jobs, refresh]);

  const updateForm = (key, value) =>
    setForm((current) => ({ ...current, [key]: value }));

  async function previewBackfill() {
    const error = validateIngestionForm(form);
    if (error) {
      setMessage({ type: "error", text: error });
      return;
    }
    const request = {
      symbols: parseSymbols(form.symbols),
      start: form.start,
      end: form.end,
    };
    if (!getBridge()) {
      setPreview(localPreview(form));
      setMessage({
        type: "success",
        text: "Preview updated. No data was downloaded.",
      });
      return;
    }
    try {
      setPreview(
        (await invoke("IngestionPreview", request)) || localPreview(form),
      );
      setMessage({
        type: "success",
        text: "Preview updated. No data was downloaded.",
      });
    } catch (error) {
      setPreview(localPreview(form));
      setMessage({ type: "error", text: error.message });
    }
  }

  async function startBackfill() {
    const error = validateIngestionForm(form);
    if (error) {
      setMessage({ type: "error", text: error });
      return;
    }
    try {
      const job = await invoke("IngestionStartBackfill", {
        list_id: "",
        symbols: parseSymbols(form.symbols),
        start: form.start,
        end: form.end,
      });
      setJobs((current) => [job, ...current]);
      setMessage({ type: "success", text: `Backfill ${job.id} queued.` });
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  async function saveList() {
    const symbols = parseSymbols(form.symbols);
    if (!form.listName.trim() || !symbols.length) {
      setMessage({
        type: "error",
        text: "Enter a list name and at least one symbol.",
      });
      return;
    }
    try {
      const list = await invoke("IngestionSaveList", {
        name: form.listName.trim(),
        symbols,
      });
      setLists((current) => [...current, list]);
      setForm((current) => ({ ...current, listName: "" }));
      setMessage({ type: "success", text: `Saved ${list.name}.` });
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  async function cancelJob(id) {
    try {
      await invoke("IngestionCancelJob", id);
      setMessage({ type: "success", text: "Cancellation requested." });
      refresh();
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  async function retryJob(id) {
    try {
      await invoke("IngestionRetryJob", id);
      setMessage({ type: "success", text: "Job retry requested." });
      refresh();
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  async function deleteList(id) {
    try {
      await invoke("IngestionDeleteList", id);
      setLists((current) => current.filter((list) => list.id !== id));
      setMessage({
        type: "success",
        text: "Symbol list deleted. Stored data was not deleted.",
      });
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  async function setSchedule(enabled) {
    const symbols = parseSymbols(form.symbols);
    try {
      await invoke("IngestionSetSchedule", {
        list_id: "",
        symbols,
        enabled,
      });
      setScheduleEnabled(enabled);
      setMessage({
        type: "success",
        text: enabled ? "Daily refresh enabled." : "Daily refresh disabled.",
      });
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  async function reviewDiscrepancy(id) {
    if (
      typeof globalThis.window?.confirm === "function" &&
      !globalThis.window.confirm(
        "Accept the listed omissions and remove those stored timestamps?",
      )
    )
      return;
    try {
      await invoke("IngestionReviewDiscrepancy", {
        candidate_id: id,
        accept_removals: true,
        note: "Reviewed in Data page",
      });
      setMessage({ type: "success", text: "Coverage discrepancy accepted." });
      refresh();
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  async function deleteData() {
    const error = validateIngestionForm(form);
    if (error) {
      setMessage({ type: "error", text: error });
      return;
    }
    const scope = `${parseSymbols(form.symbols).join(", ")} from ${form.start} to ${form.end}`;
    if (
      typeof globalThis.window?.confirm === "function" &&
      !globalThis.window.confirm(`Delete stored data for ${scope}?`)
    )
      return;
    try {
      const result = await invoke("IngestionDeleteData", {
        symbols: parseSymbols(form.symbols),
        start: form.start,
        end: form.end,
      });
      setMessage({
        type: "success",
        text: result.message || "Deletion requested.",
      });
      refresh();
    } catch (error) {
      setMessage({ type: "error", text: error.message });
    }
  }

  return {
    form,
    updateForm,
    status,
    lists,
    jobs,
    preview,
    message,
    setMessage,
    previewBackfill,
    startBackfill,
    saveList,
    cancelJob,
    retryJob,
    deleteList,
    deleteData,
    scheduleEnabled,
    setSchedule,
    reviewDiscrepancy,
    refresh,
  };
}
