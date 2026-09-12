import { useState } from "react";
import { initialConfig, validateConfig } from "./config.js";

export function useBacktest() {
  const [config, setConfig] = useState(initialConfig),
    [detail, setDetail] = useState(null),
    [message, setMessage] = useState(null);
  function run() {
    const error = validateConfig(config);
    setMessage(
      error
        ? { title: "Check run settings", text: error }
        : {
            title: "Backtest execution is not connected",
            text: `Your selection contains ${config.symbols.length * config.strategies.length} backtests for ${config.start} to ${config.end}. This layout preview uses sample results; it has not run your configuration.`,
          },
    );
  }
  return { config, setConfig, detail, setDetail, message, setMessage, run };
}
