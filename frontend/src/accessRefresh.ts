export function createFocusAccessRefresh(refresh: () => Promise<void>, {
  now = () => performance.now(), lastLoad = () => -Infinity, interval = 10000,
}: {
  now?: () => number;
  lastLoad?: () => number;
  interval?: number
} = {}) {
  let lastFocus = -Infinity, inFlight = false;
  return (visible: boolean) => {
    const time = now();
    if (!visible || inFlight || time - Math.max(lastFocus, lastLoad()) < interval) return false;
    lastFocus = time;
    inFlight = true;
    void Promise.resolve().then(refresh).catch(() => undefined).finally(() => { inFlight = false; });
    return true;
  };
}
