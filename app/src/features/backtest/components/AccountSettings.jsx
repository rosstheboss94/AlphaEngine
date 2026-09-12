import React from "react";
import { Wallet } from "lucide-react";
import { Field, Section } from "./SettingsFields.jsx";
import AccountInput from "./AccountInput.jsx";

export default function AccountSettings({ config, setConfig }) {
  const update = (key, value) => setConfig((c) => ({ ...c, [key]: value }));
  return (
    <Section title="Account" icon={Wallet}>
      <div className="space-y-2.5">
        {[
          ["cash", "Initial cash", "$", ""],
          ["target", "Buy target", "$", ""],
          ["slippage", "Slippage", "", "bps"],
        ].map(([key, label, prefix, suffix]) => (
          <Field key={key} label={label}>
            <div className="control flex items-center gap-1">
              {prefix && <span>{prefix}</span>}
              <AccountInput
                label={label}
                value={config[key]}
                monetary={Boolean(prefix)}
                onChange={(value) => update(key, value)}
              />
              {suffix && <span className="text-secondary">{suffix}</span>}
            </div>
          </Field>
        ))}
      </div>
      <p className="account-note text-sm text-secondary">
        Account settings apply to each backtest.
      </p>
    </Section>
  );
}
