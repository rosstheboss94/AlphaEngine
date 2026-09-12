import React from "react";
import DataSettings from "./DataSettings.jsx";
import StrategySettings from "./StrategySettings.jsx";
import AccountSettings from "./AccountSettings.jsx";

export default function SettingsSidebar({ config, setConfig }) {
  return (
    <aside className="sidebar shrink-0 overflow-y-auto border-r border-line">
      <DataSettings config={config} setConfig={setConfig} />
      <StrategySettings config={config} setConfig={setConfig} />
      <AccountSettings config={config} setConfig={setConfig} />
    </aside>
  );
}
