/**
 * Contador de Visitas Reales y Telemetría Persistente en Cloud Firestore
 * Registra visitas reales en base de datos persistente compartida entre todas las URLs y dispositivos.
 */

import {
  collection,
  doc,
  getDoc,
  getDocs,
  limit,
  onSnapshot,
  orderBy,
  query,
  runTransaction,
  serverTimestamp,
  setDoc,
  Timestamp,
} from "firebase/firestore";
import { auth, db, handleFirestoreError, isFirebaseAdminUser, isFirebaseConfigured, OperationType } from "./firebase";
import { safeStorage, safeSessionStorage } from "./utils/storage";

const STORAGE_VISITS_KEY = "antena_universal_visits_count";
const STORAGE_REAL_VISITS_KEY = "antena_real_firestore_visits_count";
const STORAGE_EVENTS_KEY = "antena_telemetry_events_log";
const LAST_INCREMENT_TIME_KEY = "antena_last_increment_time_epoch";
const SESSION_COUNTED_KEY = "antena_visit_session_counted";
const SESSION_HASH_KEY = "antena_visitor_session_hash";
const VISIT_DEBOUNCE_MS = 30 * 1000; // Evita duplicar si el usuario abre 2 pestañas simultáneas en 30s
const SERVER_POLL_INTERVAL_MS = 15 * 1000;

let inFlightVisitPromise: Promise<number> | null = null;

export interface TelemetryEvent {
  id: string;
  timestamp: string;
  type: "visita" | "sintonizacion" | "consulta" | "grabacion" | "transmision";
  details: string;
  device: string;
}

export interface ServerVisitLog {
  id: string;
  timestamp: string;
  device: string;
  host?: string;
  path?: string;
}

export interface ServerVisitStats {
  visits: number;
  realVerifiedCount: number;
  updatedAt?: string;
  recentVisits: ServerVisitLog[];
}

const sanitizeId = (raw: string, maxLen = 64): string => {
  const cleaned = raw.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, maxLen);
  return cleaned || "anon_" + Date.now().toString(36);
};

const getSessionHash = (): string => {
  if (typeof window === "undefined") return "ssr_session";
  let existing = safeStorage.getItem(SESSION_HASH_KEY);
  if (!existing) {
    existing = sanitizeId(
      "sess_" + Date.now().toString(36) + "_" + Math.random().toString(36).substring(2, 10),
      64
    );
    safeStorage.setItem(SESSION_HASH_KEY, existing);
  }
  return sanitizeId(existing, 64);
};

const formatFirestoreTimestamp = (ts: unknown): string => {
  if (ts instanceof Timestamp) {
    return ts.toDate().toISOString();
  }
  if (ts && typeof (ts as any).toDate === "function") {
    return (ts as any).toDate().toISOString();
  }
  if (typeof ts === "string") return ts;
  return new Date().toISOString();
};

// Obtener info básica del dispositivo del visitante
export const getDeviceInfo = (): string => {
  if (typeof window === "undefined" || !navigator) return "Navegador Web";
  const ua = navigator.userAgent;
  let device = "Escritorio (PC/Mac)";
  if (/Android/i.test(ua)) device = "Móvil (Android)";
  else if (/iPhone|iPad|iPod/i.test(ua)) device = "Móvil (iOS)";
  else if (/Mobile/i.test(ua)) device = "Dispositivo Móvil";

  let browser = "Navegador";
  if (/Chrome/i.test(ua) && !/Edg/i.test(ua)) browser = "Chrome";
  else if (/Safari/i.test(ua) && !/Chrome/i.test(ua)) browser = "Safari";
  else if (/Firefox/i.test(ua)) browser = "Firefox";
  else if (/Edg/i.test(ua)) browser = "Edge";

  return `${device} — ${browser}`.slice(0, 100);
};

// Obtener lista de eventos de telemetría guardados localmente
export const getLocalTelemetryLogs = (): TelemetryEvent[] => {
  if (typeof window === "undefined") return [];
  try {
    const raw = safeStorage.getItem(STORAGE_EVENTS_KEY);
    if (raw) return JSON.parse(raw);
  } catch (e) {}
  return [];
};

// Registrar un nuevo evento de telemetría local
export const logTelemetryEvent = (
  type: TelemetryEvent["type"],
  details: string
): TelemetryEvent[] => {
  if (typeof window === "undefined") return [];
  try {
    const current = getLocalTelemetryLogs();
    const newEvent: TelemetryEvent = {
      id: "evt_" + Date.now() + "_" + Math.random().toString(36).substring(2, 6),
      timestamp: new Date().toLocaleTimeString("es-ES", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        day: "2-digit",
        month: "2-digit",
      }),
      type,
      details,
      device: getDeviceInfo(),
    };
    const updated = [newEvent, ...current].slice(0, 50);
    safeStorage.setItem(STORAGE_EVENTS_KEY, JSON.stringify(updated));
    return updated;
  } catch (e) {
    return [];
  }
};

// Obtener conteo guardado en caché local
export const getStoredVisits = (): number => {
  if (typeof window === "undefined") return 303;
  try {
    const val = safeStorage.getItem(STORAGE_VISITS_KEY);
    if (val) {
      const parsed = parseInt(val, 10);
      if (!isNaN(parsed) && parsed >= 0) return parsed;
    }
  } catch (e) {}
  return 303;
};

export const getStoredRealVisits = (): number => {
  if (typeof window === "undefined") return 0;
  try {
    const val = safeStorage.getItem(STORAGE_REAL_VISITS_KEY);
    if (val) {
      const parsed = parseInt(val, 10);
      if (!isNaN(parsed) && parsed >= 0) return parsed;
    }
  } catch (e) {}
  return 0;
};

// Guardar conteo en caché local
export const saveStoredVisits = (count: number, realVerifiedCount?: number): number => {
  const safeVal = Math.max(0, Math.floor(count));
  if (typeof window !== "undefined") {
    try {
      safeStorage.setItem(STORAGE_VISITS_KEY, String(safeVal));
      if (typeof realVerifiedCount === "number" && !isNaN(realVerifiedCount)) {
        safeStorage.setItem(STORAGE_REAL_VISITS_KEY, String(Math.max(0, Math.floor(realVerifiedCount))));
      }
    } catch (e) {}
  }
  return safeVal;
};

// Comprueba si el operador actual está excluido del conteo
export const isCurrentOperatorExcluded = (): boolean => {
  if (typeof window === "undefined") return false;
  try {
    const params = new URLSearchParams(window.location.search);
    if (params.get("exclude") === "true") {
      setOperatorExclusionState(true);
      return true;
    }
    if (params.get("include") === "true") {
      setOperatorExclusionState(false);
      return false;
    }
  } catch (e) {}

  if (safeStorage.getItem("antena_master_admin") !== "true") {
    return false;
  }
  return safeStorage.getItem("antena_operator_excluded") === "true";
};

// Activa o desactiva la exclusión del operador
export const setOperatorExclusionState = (excluded: boolean): boolean => {
  if (typeof window !== "undefined") {
    try {
      safeStorage.setItem("antena_operator_excluded", String(excluded));
      safeStorage.setItem("antena_exclude_my_visits_v2", String(excluded));
      safeStorage.setItem("antena_exclude_my_visits", String(excluded));
    } catch (e) {}
  }
  return excluded;
};

const parseApiStatsPayload = (data: any): ServerVisitStats => {
  const recentVisits: ServerVisitLog[] = Array.isArray(data?.recentVisits)
    ? data.recentVisits.map((v: any) => ({
        id: typeof v?.id === "string" ? v.id : "v_" + Math.random().toString(36).slice(2, 8),
        timestamp: typeof v?.timestamp === "string" ? v.timestamp : new Date().toISOString(),
        device: typeof v?.device === "string" ? v.device : "Navegador Web",
        host: typeof v?.host === "string" ? v.host : undefined,
        path: typeof v?.path === "string" ? v.path : undefined,
      }))
    : [];

  const visitsNum = typeof data?.visits === "number" && !isNaN(data.visits) ? data.visits : getStoredVisits();
  const realNum =
    typeof data?.realVerifiedCount === "number" && !isNaN(data.realVerifiedCount)
      ? data.realVerifiedCount
      : recentVisits.length || getStoredRealVisits();

  const savedVisits = saveStoredVisits(visitsNum, realNum);
  return {
    visits: savedVisits,
    realVerifiedCount: Math.max(0, Math.floor(realNum)),
    updatedAt: typeof data?.updatedAt === "string" ? data.updatedAt : undefined,
    recentVisits,
  };
};

const fetchApiVisitStats = async (): Promise<ServerVisitStats> => {
  const res = await fetch("/api/visits", { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  const data = await res.json();
  return parseApiStatsPayload(data);
};

const incrementApiVisit = async (deviceInfo: string, host: string, path: string): Promise<number> => {
  const res = await fetch("/api/visits/increment", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ device: deviceInfo, host, path }),
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  const data = await res.json();
  const parsed = parseApiStatsPayload(data);
  return parsed.visits;
};

const setApiVisits = async (val: number): Promise<number> => {
  const res = await fetch("/api/visits/set", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ count: val }),
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  const data = await res.json();
  const parsed = parseApiStatsPayload(data);
  return parsed.visits;
};

// Obtiene conteo real y registros recientes en vivo desde Firestore o Servidor Node.js
export const fetchServerVisitStats = async (): Promise<ServerVisitStats> => {
  if (!isFirebaseConfigured) {
    try {
      return await fetchApiVisitStats();
    } catch {
      return {
        visits: getStoredVisits(),
        realVerifiedCount: getStoredRealVisits(),
        recentVisits: [],
      };
    }
  }

  let count = getStoredVisits();
  let realVerifiedCount = getStoredRealVisits();
  let recentVisits: ServerVisitLog[] = [];
  let updatedAt: string | undefined;
  let firestoreSucceeded = false;

  const statsPath = "site_stats/global";
  try {
    const statsSnap = await getDoc(doc(db, "site_stats", "global"));
    if (statsSnap.exists()) {
      const data = statsSnap.data();
      if (typeof data.count === "number") {
        realVerifiedCount = typeof data.realVerifiedCount === "number" ? data.realVerifiedCount : 0;
        count = saveStoredVisits(data.count, realVerifiedCount);
        firestoreSucceeded = true;
      }
      if (data.updatedAt) {
        updatedAt = formatFirestoreTimestamp(data.updatedAt);
      }
    }
  } catch (error) {
    try {
      handleFirestoreError(error, OperationType.GET, statsPath);
    } catch {
      // Fallback to Express API below
    }
  }

  const eventsPath = "visit_events";
  try {
    const eventsQuery = query(
      collection(db, "visit_events"),
      orderBy("createdAt", "desc"),
      limit(25)
    );
    const eventsSnap = await getDocs(eventsQuery);
    recentVisits = eventsSnap.docs.map((d) => {
      const data = d.data();
      return {
        id: d.id,
        timestamp: formatFirestoreTimestamp(data.createdAt),
        device: typeof data.device === "string" ? data.device : "Navegador Web",
        host: typeof data.host === "string" ? data.host : undefined,
        path: typeof data.path === "string" ? data.path : undefined,
      };
    });
    firestoreSucceeded = true;
  } catch (error) {
    try {
      handleFirestoreError(error, OperationType.LIST, eventsPath);
    } catch {
      // Fallback to Express API below
    }
  }

  if (!firestoreSucceeded) {
    try {
      return await fetchApiVisitStats();
    } catch {
      // Ignore and return cached
    }
  }

  return { visits: count, realVerifiedCount, updatedAt, recentVisits };
};

// Suscripción en tiempo real a Firestore o polling en vivo al servidor
export const subscribeToVisitStats = (
  onUpdate: (stats: ServerVisitStats) => void
): (() => void) => {
  if (!isFirebaseConfigured) {
    let disposed = false;
    const syncNow = () => {
      fetchApiVisitStats()
        .then((stats) => {
          if (!disposed) onUpdate(stats);
        })
        .catch(() => {});
    };

    syncNow();
    const intervalId = setInterval(syncNow, SERVER_POLL_INTERVAL_MS);

    return () => {
      disposed = true;
      clearInterval(intervalId);
    };
  }

  let latestStats = {
    visits: getStoredVisits(),
    realVerifiedCount: getStoredRealVisits(),
    updatedAt: undefined as string | undefined,
  };
  let latestLogs: ServerVisitLog[] = [];

  const emit = () => {
    onUpdate({
      visits: latestStats.visits,
      realVerifiedCount: latestStats.realVerifiedCount,
      updatedAt: latestStats.updatedAt,
      recentVisits: latestLogs,
    });
  };

  const unsubStats = onSnapshot(
    doc(db, "site_stats", "global"),
    (snap) => {
      if (snap.exists()) {
        const data = snap.data();
        const count = typeof data.count === "number" ? data.count : latestStats.visits;
        const realCount = typeof data.realVerifiedCount === "number" ? data.realVerifiedCount : 0;
        saveStoredVisits(count, realCount);
        latestStats = {
          visits: count,
          realVerifiedCount: realCount,
          updatedAt: data.updatedAt ? formatFirestoreTimestamp(data.updatedAt) : undefined,
        };
        emit();
      }
    },
    (error) => {
      console.warn("Firestore stats listener notice, syncing via server API:", error);
      fetchApiVisitStats().then(onUpdate).catch(() => {});
    }
  );

  const eventsQuery = query(
    collection(db, "visit_events"),
    orderBy("createdAt", "desc"),
    limit(25)
  );
  const unsubEvents = onSnapshot(
    eventsQuery,
    (snap) => {
      latestLogs = snap.docs.map((d) => {
        const data = d.data();
        return {
          id: d.id,
          timestamp: formatFirestoreTimestamp(data.createdAt),
          device: typeof data.device === "string" ? data.device : "Navegador Web",
          host: typeof data.host === "string" ? data.host : undefined,
          path: typeof data.path === "string" ? data.path : undefined,
        };
      });
      emit();
    },
    (error) => {
      console.warn("Firestore events listener notice:", error);
    }
  );

  return () => {
    unsubStats();
    unsubEvents();
  };
};

// Sincroniza y obtiene el conteo real en vivo
export const fetchServerVisits = async (): Promise<number> => {
  const stats = await fetchServerVisitStats();
  return stats.visits;
};

// Registrar visita real atómicamente (1 vez por sesión de pestaña, o forzada manualmente)
export const registerUniversalVisit = async (force: boolean = false): Promise<number> => {
  if (!force && inFlightVisitPromise) {
    return inFlightVisitPromise;
  }

  const run = async (): Promise<number> => {
    const isExcluded = isCurrentOperatorExcluded();

    if (isExcluded && !force) {
      return fetchServerVisits();
    }

    const now = Date.now();
    const alreadyCountedThisSession = safeSessionStorage.getItem(SESSION_COUNTED_KEY) === "true";
    const lastTime = parseInt(safeStorage.getItem(LAST_INCREMENT_TIME_KEY) || "0", 10);
    const debounceExpired = isNaN(lastTime) || now - lastTime > VISIT_DEBOUNCE_MS;

    const shouldIncrement = force || (!alreadyCountedThisSession && debounceExpired);

    if (!shouldIncrement) {
      return fetchServerVisits();
    }

    const deviceInfo = getDeviceInfo();
    const rawHost = typeof window !== "undefined" ? window.location.hostname || "web" : "web";
    const rawPath = typeof window !== "undefined" ? window.location.pathname || "/" : "/";
    const host = rawHost.slice(0, 120) || "web";
    const path = rawPath.slice(0, 120) || "/";

    const markCounted = () => {
      safeSessionStorage.setItem(SESSION_COUNTED_KEY, "true");
      safeStorage.setItem(LAST_INCREMENT_TIME_KEY, String(Date.now()));
    };

    if (isFirebaseConfigured) {
      const sessionHash = getSessionHash();
      const visitId = sanitizeId(
        "v_" + Date.now().toString(36) + "_" + Math.random().toString(36).substring(2, 8),
        128
      );

      const statsRef = doc(db, "site_stats", "global");
      const visitRef = doc(db, "visit_events", visitId);

      try {
        const result = await runTransaction(db, async (tx) => {
          const statsSnap = await tx.get(statsRef);
          let nextCount = 304;
          let nextRealCount = 1;

          if (statsSnap.exists()) {
            const data = statsSnap.data();
            const curCount = typeof data.count === "number" ? data.count : 303;
            const curReal = typeof data.realVerifiedCount === "number" ? data.realVerifiedCount : 0;
            nextCount = curCount + 1;
            nextRealCount = curReal + 1;

            tx.update(statsRef, {
              count: nextCount,
              realVerifiedCount: nextRealCount,
              lastVisitId: visitId,
              updatedAt: serverTimestamp(),
            });
          } else {
            tx.set(statsRef, {
              count: 304,
              realVerifiedCount: 1,
              lastVisitId: visitId,
              updatedAt: serverTimestamp(),
            });
          }

          tx.set(visitRef, {
            device: deviceInfo,
            host,
            path,
            sessionHash,
            createdAt: serverTimestamp(),
          });

          return { nextCount, nextRealCount };
        });

        markCounted();
        logTelemetryEvent("visita", `Nueva visita real registrada desde ${deviceInfo} (${host})`);
        return saveStoredVisits(result.nextCount, result.nextRealCount);
      } catch (error) {
        console.warn("Firestore visit increment failed, using server API fallback:", error);
      }
    }

    try {
      const nextVisits = await incrementApiVisit(deviceInfo, host, path);
      markCounted();
      logTelemetryEvent("visita", `Nueva visita real registrada desde ${deviceInfo} (${host})`);
      return nextVisits;
    } catch {
      return getStoredVisits();
    }
  };

  if (force) {
    return run();
  }

  inFlightVisitPromise = run().finally(() => {
    inFlightVisitPromise = null;
  });
  return inFlightVisitPromise;
};

// Forzar incremento explícito (+1 de prueba o botón directo del administrador)
export const incrementServerVisit = async (): Promise<number> => {
  return registerUniversalVisit(true);
};

// Calibrar o establecer el conteo manualmente desde la consola de operador
export const setUniversalVisits = async (newCount: number): Promise<number> => {
  const val = Math.max(0, Math.min(100000000, Math.floor(newCount)));

  if (!isFirebaseConfigured) {
    const saved = await setApiVisits(val);
    logTelemetryEvent("visita", `Contador calibrado en servidor a ${saved}`);
    return saved;
  }

  if (!isFirebaseAdminUser(auth.currentUser)) {
    throw new Error("ADMIN_AUTH_REQUIRED");
  }

  saveStoredVisits(val);
  logTelemetryEvent("visita", `Contador calibrado en Firestore a ${val}`);

  const statsRef = doc(db, "site_stats", "global");
  try {
    const snap = await getDoc(statsRef);
    const currentReal = snap.exists() && typeof snap.data().realVerifiedCount === "number"
      ? snap.data().realVerifiedCount
      : 0;
    await setDoc(statsRef, {
      count: val,
      realVerifiedCount: currentReal,
      lastVisitId: "admin_calibration",
      updatedAt: serverTimestamp(),
    });
    return saveStoredVisits(val, currentReal);
  } catch (error) {
    handleFirestoreError(error, OperationType.WRITE, "site_stats/global");
    return val;
  }
};
