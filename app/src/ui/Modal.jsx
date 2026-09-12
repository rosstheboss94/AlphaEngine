import React, { useEffect, useRef } from "react";
import { X } from "lucide-react";
import Icon from "./Icon.jsx";

export default function Modal({ title, children, onClose }) {
  const ref = useRef(null);
  useEffect(() => {
    const el = ref.current;
    el.showModal();
    return () => el.close();
  }, []);
  return (
    <dialog
      ref={ref}
      className="modal"
      onCancel={onClose}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
      aria-labelledby="dialog-title"
    >
      <div className="flex items-center justify-between border-b border-line px-6 py-5">
        <h2 id="dialog-title" className="text-xl font-semibold">
          {title}
        </h2>
        <button
          autoFocus
          className="icon-button"
          aria-label="Close dialog"
          onClick={onClose}
        >
          <Icon type={X} />
        </button>
      </div>
      <div className="p-6">{children}</div>
    </dialog>
  );
}
