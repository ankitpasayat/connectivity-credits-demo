import type { Data } from "./types";
export async function api<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const detail = payload?.detail;
    throw new Error(
      typeof detail === "string"
        ? detail
        : Array.isArray(detail)
          ? detail
              .map(
                (e: { msg?: string; loc?: string[] }) =>
                  `${e.loc?.slice(1).join(".") || "Input"}: ${e.msg}`,
              )
              .join("; ")
          : `Request failed (${response.status}).`,
    );
  }
  return payload as T;
}
export async function fetchData(): Promise<Data> {
  const [
    overview,
    schools,
    providers,
    measurements,
    credits,
    events,
    rules,
    health,
  ] = await Promise.all([
    api<Data["overview"]>("/overview"),
    api<Data["schools"]>("/schools"),
    api<Data["providers"]>("/providers"),
    api<Data["measurements"]>("/measurements"),
    api<Data["credits"]>("/credits"),
    api<Data["events"]>("/events"),
    api<Data["rules"]>("/rules"),
    api<Data["health"]>("/health"),
  ]);
  return {
    overview,
    schools,
    providers,
    measurements,
    credits,
    events,
    rules,
    health,
  };
}
