export function revealLaunchElement(element: HTMLElement | null, focusSelector?: string) {
  if (!element) return;
  element.scrollIntoView(
    {
      block: "center",
      behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth"
    }
  );
  const target = focusSelector ? element.querySelector<HTMLElement>(focusSelector) || element : element;
  target.focus({ preventScroll: true });
}
