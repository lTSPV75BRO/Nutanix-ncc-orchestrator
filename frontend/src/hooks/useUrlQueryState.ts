import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

export function useUrlQueryState(key: string, fallback = "") {
  const [searchParams, setSearchParams] = useSearchParams();
  const value = useMemo(() => (searchParams.get(key) || "").trim() || fallback, [searchParams, key, fallback]);

  const setValue = (nextValue: string, opts?: { replace?: boolean; dropIfFallback?: boolean }) => {
    const next = new URLSearchParams(searchParams);
    const clean = (nextValue || "").trim();
    const dropIfFallback = opts?.dropIfFallback ?? true;
    if (!clean || (dropIfFallback && clean === fallback)) {
      next.delete(key);
    } else {
      next.set(key, clean);
    }
    setSearchParams(next, { replace: opts?.replace ?? true });
  };

  return [value, setValue] as const;
}

