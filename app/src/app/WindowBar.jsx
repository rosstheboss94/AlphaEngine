import React from "react";
import { ChartNoAxesColumnIncreasing, Minus, Square, X } from "lucide-react";
import Icon from "../ui/Icon.jsx";
import { getWindowControls } from "../platform/windowControls.js";

export default function WindowBar() {
  const controls = getWindowControls();
  return (
    <div className="titlebar flex h-[46px] shrink-0 items-center justify-between border-b border-line pl-5">
      <div className="flex items-center gap-5 text-[18px]">
        <Icon type={ChartNoAxesColumnIncreasing} className="text-accent" />
        Backtest Engine
      </div>
      <div className="window-actions flex h-full">
        {[
          [Minus, "Minimise", "minimise"],
          [Square, "Maximise or restore", "toggleMaximise"],
          [X, "Close", "quit"],
        ].map(([icon, label, method]) => (
          <button
            key={method}
            aria-label={label}
            title={controls.available ? label : "Available in the desktop app"}
            disabled={!controls.available}
            className={`w-16 flex items-center justify-center disabled:opacity-35 hover:bg-white/8 ${method === "quit" ? "hover:!bg-red-600" : ""}`}
            onClick={() => controls[method]()}
          >
            <Icon type={icon} size={16} />
          </button>
        ))}
      </div>
    </div>
  );
}
