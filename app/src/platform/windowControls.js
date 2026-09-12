export function getWindowControls() {
  const runtime = window.runtime;
  return {
    available: Boolean(runtime),
    minimise: () => runtime?.WindowMinimise?.(),
    toggleMaximise: () => runtime?.WindowToggleMaximise?.(),
    quit: () => runtime?.Quit?.(),
  };
}
