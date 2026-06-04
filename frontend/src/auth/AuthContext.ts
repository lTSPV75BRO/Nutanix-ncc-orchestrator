import { createContext, useContext } from "react";
import type { MeData, UserRole } from "../api/types";

export type AuthValue = {
  me: MeData | null;
  role: UserRole;
  isAdmin: boolean;
  canOperate: boolean;
  loginEnabled: boolean;
  authenticated: boolean;
};

const defaultAuth: AuthValue = {
  me: null,
  role: "",
  isAdmin: true, // before /auth/me resolves, assume full access (single-user/no-login deployments)
  canOperate: true,
  loginEnabled: false,
  authenticated: false,
};

export const AuthContext = createContext<AuthValue>(defaultAuth);

export const useAuth = (): AuthValue => useContext(AuthContext);
