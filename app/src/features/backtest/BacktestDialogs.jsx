import React from "react";
import { Info } from "lucide-react";
import Icon from "../../ui/Icon.jsx";
import Modal from "../../ui/Modal.jsx";
import ResultDialog from "./components/ResultDialog.jsx";

export function BacktestDialogs({ detail, setDetail, message, setMessage }) {
  return (
    <>
      {detail && <ResultDialog row={detail} onClose={() => setDetail(null)} />}{" "}
      {message && (
        <Modal title={message.title} onClose={() => setMessage(null)}>
          <div className="flex gap-3">
            <Icon type={Info} className="text-accent shrink-0 mt-1" />
            <p className="text-secondary leading-7">{message.text}</p>
          </div>
          <button
            className="button mt-6 ml-auto"
            onClick={() => setMessage(null)}
          >
            Close
          </button>
        </Modal>
      )}
    </>
  );
}
