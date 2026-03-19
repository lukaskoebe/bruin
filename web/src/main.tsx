import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { TooltipProvider } from "@/components/ui/tooltip";
import "./monaco-loader";
import { AppRouter } from "./router";
import "./globals.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <TooltipProvider>
      <AppRouter />
    </TooltipProvider>
  </StrictMode>
);
