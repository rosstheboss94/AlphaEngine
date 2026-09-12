import React from "react";
import { FlaskConical } from "lucide-react";
import Icon from "../ui/Icon.jsx";

export default function Footer() {
  return (
    <footer className="app-footer px-5 shrink-0 border-t border-line flex justify-between items-center gap-3 text-sm text-secondary">
      <span>price-only; input adjustment status unknown</span>
      <span className="flex items-center gap-2">
        <Icon type={FlaskConical} size={14} />
        Illustrative results
      </span>
    </footer>
  );
}
