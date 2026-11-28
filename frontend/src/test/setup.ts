import "@testing-library/jest-dom/vitest";
import { cleanup, configure } from "@testing-library/react";
import { afterEach } from "vitest";

// waitFor and findBy* give up after one second by default, which a loaded
// machine (the full suite in parallel, a CI runner) can exceed on an Ant Design
// render. Passing tests are not slower: both return as soon as they succeed.
configure({ asyncUtilTimeout: 5_000 });

// Testing Library only unmounts automatically when Vitest globals are on, and
// they are off here. Without this, every rendered tree stays mounted after its
// test: React Query fetches and timers keep running, and when the file ends
// jsdom is torn down under them ("window is not defined" from React's
// scheduler, seen on CI's slower Linux runners).
afterEach(() => {
  cleanup();
});

function createStorage(): Storage {
  const values = new Map<string, string>();
  return {
    get length() {
      return values.size;
    },
    clear: () => values.clear(),
    getItem: (key: string) => values.get(key) ?? null,
    key: (index: number) => [...values.keys()][index] ?? null,
    removeItem: (key: string) => values.delete(key),
    setItem: (key: string, value: string) => values.set(key, String(value)),
  };
}

if (typeof window !== "undefined") {
  Object.defineProperty(window, "localStorage", { configurable: true, value: createStorage() });
  Object.defineProperty(window, "sessionStorage", { configurable: true, value: createStorage() });
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: window.localStorage });
  Object.defineProperty(globalThis, "sessionStorage", { configurable: true, value: window.sessionStorage });
}

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}

if (typeof globalThis.ResizeObserver === "undefined") {
  // Ant Design table internals depend on ResizeObserver in jsdom tests.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (globalThis as any).ResizeObserver = ResizeObserverMock;
}

if (typeof window !== "undefined" && typeof window.matchMedia !== "function") {
  window.matchMedia = (query: string): MediaQueryList => ({
    media: query,
    matches: false,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  });
}
