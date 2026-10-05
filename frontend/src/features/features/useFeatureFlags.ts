import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";

export function useFeatureFlags() {
  return useQuery({
    queryKey: ["feature-flags"],
    queryFn: api.featureFlags,
    staleTime: 15_000,
  });
}
