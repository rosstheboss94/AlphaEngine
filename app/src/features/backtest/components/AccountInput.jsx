import React, { useState } from "react";

export default function AccountInput({ label, value, monetary, onChange }) {
  const [focused, setFocused] = useState(false);
  const display =
    monetary && !focused && /^\d+(?:\.\d*)?$/.test(value)
      ? Number(value).toLocaleString("en-US", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        })
      : value;
  return (
    <input
      aria-label={label}
      className="w-full min-w-0 bg-transparent outline-none"
      inputMode="decimal"
      value={display}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}
