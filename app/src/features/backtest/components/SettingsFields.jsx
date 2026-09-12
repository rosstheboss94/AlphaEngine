import React from "react";
import Icon from "../../../ui/Icon.jsx";

export function Field({ label, children }) {
  return (
    <div className="field">
      <span>{label}</span>
      {children}
    </div>
  );
}
export function Section({ title, icon, children }) {
  return (
    <section className="sidebar-section">
      <h2 className="section-title flex items-center gap-4 text-[21px] font-semibold">
        <Icon type={icon} size={23} />
        {title}
      </h2>
      {children}
    </section>
  );
}
