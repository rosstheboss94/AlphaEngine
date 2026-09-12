import React from "react";

const iconProps = { size: 20, strokeWidth: 1.6, "aria-hidden": true };
export default function Icon({ type: Type, ...props }) {
  return <Type {...iconProps} {...props} />;
}
