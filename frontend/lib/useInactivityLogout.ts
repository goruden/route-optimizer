/**
 * useInactivityLogout.ts
 *
 * Tracks user activity (mouse, keyboard, touch, scroll).
 * After `timeoutMinutes` of silence → calls onLogout().
 * After `warningMinutes` of silence → calls onWarning() once
 * so the UI can show a "You'll be logged out in 5 min" dialog.
 *
 * Usage:
 *   useInactivityLogout({
 *     timeoutMinutes: 30,
 *     warningMinutes: 5,
 *     onLogout:  handleLogout,
 *     onWarning: () => setShowWarning(true),
 *     onActivity: refreshTokenIfNearExpiry,
 *   });
 */

import { useEffect, useRef, useCallback } from "react";

const ACTIVITY_EVENTS = [
  "mousemove",
  "mousedown",
  "keydown",
  "touchstart",
  "scroll",
  "click",
  "wheel",
] as const;

interface Options {
  /** Total idle time before logout. Default: 30 min */
  timeoutMinutes?: number;
  /** Show a warning this many minutes before logout. Default: 5 min */
  warningMinutes?: number;
  /** Called when idle timeout is reached — log the user out */
  onLogout: () => void;
  /** Called once when entering the warning period */
  onWarning?: () => void;
  /** Called on every user activity event (throttled to once per 30 s) */
  onActivity?: () => void;
  /** Set false to disable the hook entirely (e.g. when not authenticated) */
  enabled?: boolean;
}

export function useInactivityLogout({
  timeoutMinutes = 30,
  warningMinutes = 5,
  onLogout,
  onWarning,
  onActivity,
  enabled = true,
}: Options) {
  const lastActivityRef  = useRef<number>(Date.now());
  const warningFiredRef  = useRef<boolean>(false);
  const lastRefreshRef   = useRef<number>(Date.now());
  const intervalRef      = useRef<ReturnType<typeof setInterval>>(undefined);

  // Stable callback so event listeners don't re-bind on every render
  const handleActivity = useCallback(() => {
    const now = Date.now();
    lastActivityRef.current = now;

    // Reset warning so it can fire again after user resumes
    warningFiredRef.current = false;

    // Throttle onActivity to once every 30 s to avoid spamming token refresh
    if (onActivity && now - lastRefreshRef.current > 30_000) {
      lastRefreshRef.current = now;
      onActivity();
    }
  }, [onActivity]);

  useEffect(() => {
    if (!enabled) return;

    // Register all activity listeners (passive = no scroll blocking)
    ACTIVITY_EVENTS.forEach(ev =>
      window.addEventListener(ev, handleActivity, { passive: true })
    );

    const timeoutMs = timeoutMinutes * 60_000;
    const warningMs = (timeoutMinutes - warningMinutes) * 60_000;

    // Poll every 15 seconds — precise enough, cheap enough
    intervalRef.current = setInterval(() => {
      const idle = Date.now() - lastActivityRef.current;

      if (idle >= timeoutMs) {
        onLogout();
      } else if (idle >= warningMs && !warningFiredRef.current) {
        warningFiredRef.current = true;
        onWarning?.();
      }
    }, 15_000);

    return () => {
      ACTIVITY_EVENTS.forEach(ev =>
        window.removeEventListener(ev, handleActivity)
      );
      clearInterval(intervalRef.current);
    };
  }, [enabled, timeoutMinutes, warningMinutes, onLogout, onWarning, handleActivity]);

  /** Call this to manually record activity (e.g. after a successful API call) */
  const resetActivity = useCallback(() => {
    lastActivityRef.current = Date.now();
    warningFiredRef.current = false;
  }, []);

  return { resetActivity };
}