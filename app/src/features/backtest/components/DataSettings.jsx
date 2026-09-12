import React, { useState } from "react";
import { Database, X, ChevronDown, Check, Plus } from "lucide-react";
import Icon from "../../../ui/Icon.jsx";
import { Field, Section } from "./SettingsFields.jsx";

export default function DataSettings({ config, setConfig }) {
  const [adding, setAdding] = useState(false),
    [symbol, setSymbol] = useState(""),
    [error, setError] = useState("");
  const update = (key, value) => setConfig((c) => ({ ...c, [key]: value }));
  const add = (e) => {
    e.preventDefault();
    const s = symbol.trim().toUpperCase();
    if (!/^[A-Z][A-Z0-9.-]{0,11}$/.test(s)) {
      setError("Enter a valid stock symbol.");
      return;
    }
    if (config.symbols.length >= 30) {
      setError("Select up to 30 symbols.");
      return;
    }
    if (!config.symbols.includes(s)) update("symbols", [...config.symbols, s]);
    setSymbol("");
    setError("");
    setAdding(false);
  };
  return (
    <Section title="Data" icon={Database}>
      <Field label="Symbols">
        <div className="control symbol-picker flex min-h-10 flex-wrap items-center gap-1.5 !p-1.5">
          {config.symbols.map((s) => (
            <span className="chip" key={s}>
              {s}
              <button
                aria-label={`Remove ${s}`}
                onClick={() =>
                  update(
                    "symbols",
                    config.symbols.filter((x) => x !== s),
                  )
                }
              >
                <Icon type={X} size={13} />
              </button>
            </span>
          ))}
          <button
            className="picker-toggle"
            aria-label="Add another symbol"
            aria-expanded={adding}
            onClick={() => setAdding(!adding)}
          >
            <Icon type={ChevronDown} size={16} />
          </button>
          {!config.symbols.length && (
            <span className="text-muted px-1">No symbols selected</span>
          )}
        </div>
      </Field>
      <div className="field-after">
        {adding ? (
          <form onSubmit={add} className="flex gap-2 mt-2">
            <input
              autoFocus
              aria-label="New symbol"
              maxLength={12}
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
              className="control min-w-0 w-full"
              placeholder="e.g. NVDA"
            />
            <button className="text-accent" aria-label="Confirm symbol">
              <Icon type={Check} />
            </button>
            <button
              type="button"
              aria-label="Cancel adding symbol"
              onClick={() => {
                setAdding(false);
                setError("");
              }}
            >
              <Icon type={X} size={17} />
            </button>
          </form>
        ) : (
          <button
            className="text-accent flex items-center gap-1 py-2 text-base"
            onClick={() => setAdding(true)}
          >
            <Icon type={Plus} size={16} />
            Add symbol
          </button>
        )}
        {error && (
          <p role="alert" className="text-negative text-xs mt-1">
            {error}
          </p>
        )}
      </div>
      <div className="date-fields space-y-3.5">
        {[
          ["start", "Start date"],
          ["end", "End date"],
        ].map(([key, label]) => (
          <Field key={key} label={label}>
            <div className="date-control">
              <span aria-hidden="true">{config[key] || "yyyy-mm-dd"}</span>
              <input
                aria-label={label}
                className="control"
                type="date"
                value={config[key]}
                onChange={(e) => update(key, e.target.value)}
              />
            </div>
          </Field>
        ))}
      </div>
    </Section>
  );
}
