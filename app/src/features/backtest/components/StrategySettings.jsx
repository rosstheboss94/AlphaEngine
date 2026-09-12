import React, { useState } from "react";
import { Settings, X, ChevronDown } from "lucide-react";
import Icon from "../../../ui/Icon.jsx";
import { Field, Section } from "./SettingsFields.jsx";
import { strategies } from "../config.js";

export default function StrategySettings({ config, setConfig }) {
  const [selecting, setSelecting] = useState(false);
  const update = (key, value) => setConfig((c) => ({ ...c, [key]: value }));
  return (
    <Section title="Strategy" icon={Settings}>
      <Field label="Strategies">
        <div>
          <div className="control strategy-picker flex flex-wrap gap-1.5 !p-1.5">
            {config.strategies.map((s) => (
              <span className="chip" key={s}>
                {s}
                <button
                  aria-label={`Remove ${s}`}
                  onClick={() =>
                    update(
                      "strategies",
                      config.strategies.filter((x) => x !== s),
                    )
                  }
                >
                  <Icon type={X} size={13} />
                </button>
              </span>
            ))}
            <button
              aria-label="Choose strategies"
              aria-expanded={selecting}
              className="picker-toggle"
              onClick={() => setSelecting(!selecting)}
            >
              <Icon type={ChevronDown} size={16} />
            </button>
          </div>
          {selecting && (
            <div className="rounded border border-line bg-panel p-3 mt-2 space-y-3">
              {strategies.map((s) => (
                <label key={s} className="flex items-center gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={config.strategies.includes(s)}
                    onChange={(e) =>
                      update(
                        "strategies",
                        e.target.checked
                          ? [...config.strategies, s]
                          : config.strategies.filter((x) => x !== s),
                      )
                    }
                  />
                  {s}
                </label>
              ))}
            </div>
          )}
        </div>
      </Field>
      <p className="field-after batch-count text-sm text-secondary">
        {config.symbols.length} symbols × {config.strategies.length} strategies
        = {config.symbols.length * config.strategies.length} backtests
      </p>
      <Field label="Interval">
        <select
          className="control"
          aria-label="Interval"
          value={config.interval}
          onChange={(e) => update("interval", e.target.value)}
        >
          {["1 minute", "5 minutes", "15 minutes", "1 hour", "1 day"].map(
            (x) => (
              <option key={x}>{x}</option>
            ),
          )}
        </select>
      </Field>
    </Section>
  );
}
