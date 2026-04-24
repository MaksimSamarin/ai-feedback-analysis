import { useEffect, useRef } from "react";
import type { CSSProperties, ReactNode } from "react";

/**
 * Горизонтальный drag-scroll мышкой: нажал ЛКМ → тянешь → таблица/контейнер
 * скроллится по X. Курсор на контейнере — `grab`, во время перетаскивания — `grabbing`.
 *
 * Зачем: у превью отчёта много динамических колонок, тонкий горизонтальный
 * скроллбар ловить неудобно. Перетаскивание работает по всей площади контейнера.
 *
 * Нюансы реализации:
 * - Порог 5px: пока курсор не сдвинулся больше порога — это обычный клик;
 *   не срываем текстовое выделение и не блокируем Tooltip-ы на ячейках.
 * - Работает только с мышью (mousedown/mousemove/mouseup). На тач-устройствах
 *   остаётся нативный скролл пальцем — не перехватываем pointer-события.
 * - При mouseleave сбрасываемся, чтобы курсор не «залипал» в grabbing.
 * - Хук можно вешать на любой скроллируемый элемент с `overflow-x: auto`.
 */
export function useGrabScroll<T extends HTMLElement>(ref: React.RefObject<T | null>): void {
  const stateRef = useRef<{
    active: boolean;
    dragging: boolean;
    startX: number;
    startScrollLeft: number;
  }>({
    active: false,
    dragging: false,
    startX: 0,
    startScrollLeft: 0,
  });

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const prevCursor = el.style.cursor;
    const prevUserSelect = el.style.userSelect;
    el.style.cursor = "grab";

    const onMouseDown = (e: MouseEvent) => {
      if (e.button !== 0) return;
      stateRef.current = {
        active: true,
        dragging: false,
        startX: e.clientX,
        startScrollLeft: el.scrollLeft,
      };
    };

    const onMouseMove = (e: MouseEvent) => {
      const s = stateRef.current;
      if (!s.active) return;
      const dx = e.clientX - s.startX;
      if (!s.dragging) {
        if (Math.abs(dx) < 5) return;
        s.dragging = true;
        el.style.cursor = "grabbing";
        el.style.userSelect = "none";
      }
      el.scrollLeft = s.startScrollLeft - dx;
      e.preventDefault();
    };

    const stopDrag = () => {
      const s = stateRef.current;
      if (!s.active) return;
      stateRef.current = { active: false, dragging: false, startX: 0, startScrollLeft: 0 };
      el.style.cursor = "grab";
      el.style.userSelect = prevUserSelect;
    };

    el.addEventListener("mousedown", onMouseDown);
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", stopDrag);
    el.addEventListener("mouseleave", stopDrag);

    return () => {
      el.removeEventListener("mousedown", onMouseDown);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", stopDrag);
      el.removeEventListener("mouseleave", stopDrag);
      el.style.cursor = prevCursor;
      el.style.userSelect = prevUserSelect;
    };
  }, [ref]);
}

/**
 * Обёртка над div с подключённым `useGrabScroll`. Удобно прокинуть вместо `<Box>`,
 * чтобы не держать ref в родительском компоненте.
 */
export function GrabScrollBox({
  children,
  style,
  className,
}: {
  children: ReactNode;
  style?: CSSProperties;
  className?: string;
}) {
  const ref = useRef<HTMLDivElement | null>(null);
  useGrabScroll(ref);
  return (
    <div ref={ref} className={className} style={{ overflowX: "auto", ...style }}>
      {children}
    </div>
  );
}
